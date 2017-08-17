package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	gc "github.com/go-check/check"

	"github.com/pippio/gazette/cloudstore"
	"github.com/pippio/gazette/topic"
)

const (
	testJournal  = "test-journal"
	testDuration = 2 * time.Second
)

var (
	testTopic = topic.Description{
		Name:              testJournal,
		Partitions:        topic.EnumeratePartitions(testJournal, 2),
		RetentionDuration: testDuration,
	}
	msg1 = testMessage{
		Foo: 1,
		Bar: "abc",
	}
	msg2 = testMessage{
		Foo: 2,
		Bar: "def",
	}
	testJournal1 = testJournal + "/part-000"
	testJournal2 = testJournal + "/part-001"
)

type testMessage struct {
	Foo int    `json:"foo"`
	Bar string `json:"bar"`
}

type RetentionSuite struct {
	cfs cloudstore.FileSystem
}

func (s *RetentionSuite) SetUpSuite(c *gc.C) {
	s.cfs = cloudstore.NewTmpFileSystem()
}

func (s *RetentionSuite) TearDownSuite(c *gc.C) {
	s.cfs.Remove(testJournal1)
	s.cfs.Remove(testJournal2)
	s.cfs.Close()
}

func (s *RetentionSuite) SetUpTest(c *gc.C) {
	writeTestMessagesToCFS(s.cfs, c)
}

func (s *RetentionSuite) TestEnforceTopicRetention(c *gc.C) {
	// Ensure enough retention duration has expired.
	time.Sleep(testDuration + time.Millisecond)

	enforceTopicRetention(&testTopic, s.cfs, c)
	checkTestMessagesAreRemoved(s.cfs, c)
}

func (s *RetentionSuite) TestMessagesAreRetained(c *gc.C) {
	// Enforce right away so files are retained.
	enforceTopicRetention(&testTopic, s.cfs, c)
	checkTestMessagesAreRetained(s.cfs, c)
}

func (s *RetentionSuite) TestNoRetentionSpecifiedRetains(c *gc.C) {
	var noRetain = topic.Description{
		Name:       testJournal,
		Partitions: topic.EnumeratePartitions(testJournal, 2),
	}
	enforceTopicRetention(&noRetain, s.cfs, c)
	checkTestMessagesAreRetained(s.cfs, c)
}

func enforceTopicRetention(topic *topic.Description, cfs cloudstore.FileSystem, c *gc.C) {
	var toDelete []cfsFragment
	var err error
	toDelete, err = appendExpiredTopicFragments(topic, cfs, toDelete)
	c.Check(err, gc.IsNil)
	c.Check(deleteFragments(cfs, toDelete), gc.IsNil)
}

func checkTestMessagesAreRemoved(cfs cloudstore.FileSystem, c *gc.C) {
	file1, err := cfs.Open(testJournal1 + "/msg1")
	c.Check(file1, gc.IsNil)
	c.Check(err, gc.NotNil)
	c.Check(os.IsNotExist(err), gc.Equals, true)

	file2, err := cfs.Open(testJournal2 + "/msg2")
	c.Check(file2, gc.IsNil)
	c.Check(err, gc.NotNil)
	c.Check(os.IsNotExist(err), gc.Equals, true)
}

func checkTestMessagesAreRetained(cfs cloudstore.FileSystem, c *gc.C) {
	file1, err := cfs.Open(testJournal1 + "/msg1")
	c.Check(file1, gc.NotNil)
	c.Check(err, gc.IsNil)

	file2, err := cfs.Open(testJournal2 + "/msg2")
	c.Check(file2, gc.NotNil)
	c.Check(err, gc.IsNil)
}

func writeTestMessagesToCFS(cfs cloudstore.FileSystem, c *gc.C) {
	c.Assert(cfs.MkdirAll(testJournal1, 0750), gc.IsNil)
	c.Assert(cfs.MkdirAll(testJournal2, 0750), gc.IsNil)

	var file1, file2 cloudstore.File
	var err error
	file1, err = cfs.OpenFile(testJournal1+"/msg1",
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	c.Assert(err, gc.IsNil)
	file2, err = cfs.OpenFile(testJournal2+"/msg2",
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	c.Assert(err, gc.IsNil)

	var msg1b, _ = json.Marshal(msg1)
	file1.Write(msg1b)
	c.Check(file1.Close(), gc.IsNil)

	var msg2b, _ = json.Marshal(msg2)
	file1.Write(msg2b)
	c.Check(file2.Close(), gc.IsNil)
}

func deleteFragments(cfs cloudstore.FileSystem, toDelete []cfsFragment) error {
	for _, frag := range toDelete {
		if err := cfs.Remove(frag.path); err != nil {
			return err
		}
	}
	return nil
}

var _ = gc.Suite(&RetentionSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
