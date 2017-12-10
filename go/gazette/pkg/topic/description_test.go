package topic

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	gc "github.com/go-check/check"
)

type TopicSuite struct{}

func (s *TopicSuite) TestRoutingRegressionFixtures(c *gc.C) {
	var desc = Description{
		Name: "a/topic",
	}
	desc.Partitions = EnumeratePartitions("a/topic", 443) // Prime number.
	desc.MappedPartition = ModuloPartitionMapping(desc.Partitions, identityRouter)

	// Expect distributed and *stable* routing across partitions.
	for key, expectedPartition := range map[string]int{
		"It always":               119,
		"seems":                   54,
		"impossible":              308,
		"until":                   97,
		"it is done.":             179,
		"- Nelson Mandela":        437,
		"Shall I compare":         84,
		"thee to":                 300,
		"a summer's day?":         214,
		"Thou art more":           135,
		"lovely and more":         118,
		"temperate - Shakespeare": 228,
	} {
		c.Check(key+"-"+desc.MappedPartition(key).String(), gc.Equals,
			fmt.Sprintf("%v-a/topic/part-%03d", key, expectedPartition))
	}
}

func (s *TopicSuite) TestModuloRoutingWithCommonFactors(c *gc.C) {
	var seed = time.Now().UnixNano()
	c.Log("seed:", seed)

	// Build three topics such that each partition count is a multiple
	// of random lesser partition factors.
	var r = rand.New(rand.NewSource(seed))
	var A, B, C = 1 + r.Int()%8, 1 + r.Int()%8, 1 + r.Int()%8

	var topics = []Description{
		{Partitions: EnumeratePartitions("a*b*c", A*B*C)},
		{Partitions: EnumeratePartitions("a*b", A*B)},
		{Partitions: EnumeratePartitions("a", A)},
	}
	for i := range topics {
		topics[i].MappedPartition = ModuloPartitionMapping(topics[i].Partitions, identityRouter)
	}

	// Expect the routing of a*b*c also determines the routing of a*b and a,
	// via modulo remainders.
	var expect = make(map[string][]string)

	for i := 0; i != C; i++ {
		for j := 0; j != B; j++ {
			for k := 0; k != A; k++ {
				journals := []string{
					fmt.Sprintf("a*b*c/part-%03d", k+j*A+i*A*B),
					fmt.Sprintf("a*b/part-%03d", k+j*A),
					fmt.Sprintf("a/part-%03d", k),
				}
				expect[journals[0]] = journals
			}
		}
	}
	for i := 0; i != 1000; i++ {
		var key = strconv.Itoa(rand.Int())

		var journals = []string{
			topics[0].MappedPartition(key).String(),
			topics[1].MappedPartition(key).String(),
			topics[2].MappedPartition(key).String(),
		}
		c.Check(journals, gc.DeepEquals, expect[journals[0]])
	}
}

func identityRouter(message Message, b []byte) []byte { return append(b, message.(string)...) }

var _ = gc.Suite(&TopicSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
