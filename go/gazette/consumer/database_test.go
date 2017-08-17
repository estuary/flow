package consumer

import (
	"io/ioutil"
	"os"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
)

type DatabaseSuite struct{}

func (s *DatabaseSuite) TestDatabase(c *gc.C) {
	path, err := ioutil.TempDir("", "database-suite")
	c.Assert(err, gc.IsNil)
	defer func() { c.Assert(os.RemoveAll(path), gc.IsNil) }()

	var logName journal.Name = "a/recovery/log"
	fsm, err := recoverylog.NewFSM(recoverylog.FSMHints{Log: logName})
	c.Assert(err, gc.IsNil)

	var result = journal.AsyncAppend{
		Ready: make(chan struct{}),
	}
	close(result.Ready)

	var writer = &journal.MockWriter{}
	writer.On("Write", logName, mock.AnythingOfType("[]uint8")).Return(&result, nil)
	writer.On("ReadFrom", logName, mock.Anything).Return(&result, nil)

	var opts = rocks.NewDefaultOptions()
	defer opts.Destroy()

	db, err := newDatabase(opts, fsm, path, writer)

	// Expect that database operations are being replicated to |logName|.
	c.Check(err, gc.IsNil)
	c.Check(writer.Calls, gc.Not(gc.HasLen), 0)

	// Populate the current transaction.
	db.writeBatch.Put([]byte("foo"), []byte("bar"))
	db.writeBatch.Put([]byte("baz"), []byte("quux"))
	c.Check(db.writeBatch.Count(), gc.Equals, 2)

	// Commit. Expect |result| is passed through as a write barrier,
	// and that writeBatch was flushed.
	barrier, err := db.commit()
	c.Check(db.writeBatch.Count(), gc.Equals, 0)
	c.Check(barrier, gc.Equals, &result)

	// Values are now reflected in the database.
	value, _ := db.GetBytes(db.readOptions, []byte("foo"))
	c.Check(string(value), gc.Equals, "bar")
	value, _ = db.GetBytes(db.readOptions, []byte("baz"))
	c.Check(string(value), gc.Equals, "quux")

	db.teardown()
}

var _ = gc.Suite(&DatabaseSuite{})
