package gazette

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

const kTopicClientRetryTimeout = time.Second * 5

var contentRangeRegexp = regexp.MustCompile("bytes\\s+([\\d+])-\\d+/\\d+")

type pendingPut struct {
	journal string
	message []byte
}

type TopicClient struct {
	ring  *Ring
	topic Topic

	puts chan pendingPut
}

func NewTopicClient(ring *Ring, topic Topic) *TopicClient {
	client := &TopicClient{
		ring:  ring,
		topic: topic,
	}
	return client
}

func (c *TopicClient) Put(message interface{}) error {
	buf, err := c.topic.Serialize(message)
	if err != nil {
		return err
	}
	// TODO(johnny): Actual journal routing.
	journal := c.topic.Name + "/part-000"

	// Header is a magic word (for de-sync detection), and a 4-byte length.
	var header = [8]byte{0x66, 0x33, 0x93, 0x36, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(header[4:], uint32(len(buf)))

	// TODO(johnny): Batch these by-journal, for better transfer efficiency.

	for i := 0; true; i++ {
		if i != 0 {
			time.Sleep(kTopicClientRetryTimeout)
		}
		ringEntries := c.ring.Route(journal)

		if len(ringEntries) == 0 {
			log.WithField("journal", journal).Warn("no ring entries found")
			continue
		}

		request, err := http.NewRequest("PUT",
			fmt.Sprintf("%v/%v", ringEntries[0].BaseURL, journal),
			io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader(buf)))
		if err != nil {
			log.WithFields(log.Fields{"journal": journal, "err": err}).
				Warn("put failed")
			continue
		}

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			log.WithFields(log.Fields{"journal": journal, "err": err}).
				Warn("put failed")
			continue
		} else if response.StatusCode != 200 {
			log.WithFields(log.Fields{
				"journal": journal, "status": response.Status}).Warn("put failed")
			continue
		}
		break
	}
	return nil
}

func (c *TopicClient) Subscribe() chan interface{} {
	// TODO(johnny): Multiplex across journals.
	journal := c.topic.Name + "/part-000"
	out := make(chan interface{})

	var buffer []byte

	var offset int64 = -1
	for i := 0; true; i++ {
		if i != 0 {
			time.Sleep(kTopicClientRetryTimeout)
		}
		ringEntries := c.ring.Route(journal)

		if len(ringEntries) == 0 {
			log.WithField("journal", journal).Warn("no ring entries found")
			continue
		}

		request, err := http.NewRequest("GET",
			fmt.Sprintf("%v/%v?offset=%v", ringEntries[0].BaseURL, journal, offset),
			nil)
		if err != nil {
			log.WithFields(log.Fields{"journal": journal, "err": err}).
				Warn("subscribe failed")
			continue
		}

		responseOffset, response, err := c.doSubscribeRequest(request)
		if err != nil {
			log.WithFields(log.Fields{"journal": journal, "err": err}).
				Warn("subscribe failed")
			continue
		} else if responseOffset != offset {
			log.WithFields(log.Fields{"journal": journal, "prevOffset": offset,
				"offset": responseOffset}).Warn("skipped to journal offset")
			offset = responseOffset
		}

		for {
			delta, err := c.readMessage(response.Body, out, &buffer)
			if err != nil {
				log.WithFields(log.Fields{"journal": journal, "err": err}).
					Warn("subscribe interrupted")
				break
			} else {
				offset += delta
			}
		}
	}
	return out
}

func (c *TopicClient) doSubscribeRequest(request *http.Request) (
	int64, *http.Response, error) {

	if response, err := http.DefaultClient.Do(request); err != nil {
		return 0, nil, err
	} else if response.StatusCode != http.StatusPartialContent {
		return 0, nil, fmt.Errorf("unexpected status %v", response.Status)
	} else if crStr := response.Header.Get("Content-Range"); crStr == "" {
		return 0, nil, fmt.Errorf("expected Content-Range header")
	} else if m := contentRangeRegexp.FindStringSubmatch(crStr); len(m) == 0 {
		return 0, nil, fmt.Errorf("invalid Content-Range '%v'", crStr)
	} else if rOffset, err := strconv.ParseInt(m[1], 10, 64); err != nil {
		return 0, nil, err
	} else {
		return rOffset, response, nil
	}
}

func (c *TopicClient) readMessage(r io.Reader, out chan interface{},
	buffer *[]byte) (int64, error) {

	var delta int

	var expectWord = [4]byte{0x66, 0x33, 0x93, 0x36}
	var header [8]byte

	// Scan until the magic word is read.
	if n, err := r.Read(header[:]); err != nil {
		return 0, err
	} else {
		delta += n
	}
	for bytes.Compare(header[:4], expectWord[:]) != 0 {
		if delta == 4 {
			log.Error("detected message de-synchronization")
		}
		// Shift and read next byte.
		copy(header[:], header[1:])

		if n, err := r.Read(header[7:]); err != nil {
			return 0, err
		} else {
			delta += n
		}
	}
	// Next 4 bytes are message size.
	length := binary.LittleEndian.Uint32(header[4:])

	if len(*buffer) < int(length) {
		*buffer = make([]byte, length)
	}
	sizedBuffer := (*buffer)[:length]

	if n, err := io.ReadFull(r, sizedBuffer); err != nil {
		return 0, err
	} else {
		delta += n
	}

	if message, err := c.topic.Parse(sizedBuffer); err != nil {
		log.WithField("err", err).Warn("failed to parse message")
	} else {
		out <- message
	}
	return int64(delta), nil
}
