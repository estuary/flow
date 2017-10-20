package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/cloudstore"
)

const (
	testJournal  = "test-journal"
	testDuration = 2 * time.Second
)

var (
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

	enforceTopicRetention(testJournal, testDuration, s.cfs, c)
	checkTestMessagesAreRemoved(s.cfs, c)
}

func (s *RetentionSuite) TestMessagesAreRetained(c *gc.C) {
	// Enforce right away so files are retained.
	enforceTopicRetention(testJournal, testDuration, s.cfs, c)
	checkTestMessagesAreRetained(s.cfs, c)
}

func (s *RetentionSuite) TestNoRetentionSpecifiedRetains(c *gc.C) {
	enforceTopicRetention(testJournal, 0, s.cfs, c)
	checkTestMessagesAreRetained(s.cfs, c)
}

func enforceTopicRetention(prefix string, dur time.Duration,
	cfs cloudstore.FileSystem, c *gc.C) {
	var toDelete []*cfsFragment
	var err error
	toDelete, err = appendExpiredFragments(prefix, dur, toDelete, cfs)
	c.Check(err, gc.IsNil)
	c.Check(deleteExpiredFrags(toDelete, cfs), gc.IsNil)
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

var _ = gc.Suite(&RetentionSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
