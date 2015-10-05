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
	topic := Description{
		Name:       "a/topic",
		Partitions: 443, // Prime number.
		RoutingKey: identityRouter,
	}
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
		c.Check(key+"-"+topic.RoutedJournal(key).String(), gc.Equals,
			fmt.Sprintf("%v-a/topic/part-%03d", key, expectedPartition))
	}
}

func (s *TopicSuite) TestModuloRoutingWithCommonFactors(c *gc.C) {
	seed := time.Now().UnixNano()
	c.Log("seed:", seed)

	// Build three topics such that each partition count is a multiple
	// of random lesser partition factors.
	r := rand.New(rand.NewSource(seed))
	A, B, C := 1+r.Int()%8, 1+r.Int()%8, 1+r.Int()%8

	topics := []Description{
		{Partitions: A * B * C, Name: "a*b*c", RoutingKey: identityRouter},
		{Partitions: A * B, Name: "a*b", RoutingKey: identityRouter},
		{Partitions: A, Name: "a", RoutingKey: identityRouter},
	}
	// Expect the routing of a*b*c also determines the routing of a*b and a,
	// via modulo remainders.
	expect := make(map[string][]string)
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
		key := strconv.Itoa(rand.Int())

		journals := []string{
			topics[0].RoutedJournal(key).String(),
			topics[1].RoutedJournal(key).String(),
			topics[2].RoutedJournal(key).String(),
		}
		c.Check(journals, gc.DeepEquals, expect[journals[0]])
	}
}

func identityRouter(message interface{}) string { return message.(string) }

var _ = gc.Suite(&TopicSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
