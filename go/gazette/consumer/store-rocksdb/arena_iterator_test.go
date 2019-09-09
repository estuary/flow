package store_rocksdb

import (
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tecbot/gorocksdb"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestArenaIterator(t *testing.T) {
	var store = newTestStore(t)
	defer store.Destroy()

	var expect = [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for _, k := range expect {
		assert.NoError(t, store.DB.Put(store.WriteOptions, k, []byte("val")))
	}

	for _, size := range []int{0, 2, 20, 200, 32 * 1024} {
		var it = AsArenaIterator(store.DB.NewIterator(store.ReadOptions), make([]byte, size))

		var actual [][]byte
		for it.SeekToFirst(); it.Valid(); it.Next() {
			actual = append(actual, append([]byte(nil), it.Key()...))
		}
		assert.NoError(t, it.Err())
		assert.Equal(t, expect, actual)

		it.Close()
	}
}

func TestArenaIteratorSeeking(t *testing.T) {
	var store = newTestStore(t)
	defer store.Destroy()

	for i := int64(100); i != 200; i++ {
		var key, val = []byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(i, 16))
		assert.NoError(t, store.DB.Put(store.WriteOptions, key, val))
	}

	for _, size := range []int{0, 2, 20 /*200, 32 * 1024*/} {
		var iter = AsArenaIterator(store.DB.NewIterator(store.ReadOptions), make([]byte, size))

		var expect = func(i int64) {
			assert.True(t, iter.Valid())
			assert.Equal(t, strconv.FormatInt(i, 10), string(iter.Key()))
			assert.Equal(t, strconv.FormatInt(i, 16), string(iter.Value()))
		}

		// Next followed by SeekToFirst works as expected.
		for i := 0; i != 2; i++ {
			iter.SeekToFirst()
			expect(100)
			iter.Next()
			expect(101)
			iter.Next()
			expect(102)
		}

		// As does Prev.
		iter.Prev()
		expect(101)
		iter.Next()
		expect(102)

		// Seek followed by Next.
		iter.Seek([]byte("132"))
		expect(132)
		iter.Next()
		expect(133)
		iter.Seek([]byte("122"))
		expect(122)
		iter.Next()
		expect(123)

		// SeekToLast followed by Prev.
		iter.SeekToLast()
		expect(199)
		iter.Prev()
		expect(198)
		iter.Next()
		expect(199)

		// Step beyond last item.
		iter.Next()
		assert.False(t, iter.Valid())

		iter.SeekToFirst()
		expect(100)
		iter.Next()
		expect(101)
		iter.Prev()
		expect(100)

		// Step before first element.
		iter.Prev()
		assert.False(t, iter.Valid())

		iter.Close()
	}
}

func TestLenPrefixParsing(t *testing.T) {
	assert.True(t, parseLenPrefix([]byte{0x00, 0x00, 0x00, 0x00}) == 0x00000000)
	assert.True(t, parseLenPrefix([]byte{0x00, 0x00, 0x02, 0x83}) == 0x00000283)
	assert.True(t, parseLenPrefix([]byte{0x01, 0x01, 0x01, 0x01}) == 0x01010101)
	assert.True(t, parseLenPrefix([]byte{0xab, 0xcd, 0xef, 0x12}) == 0xabcdef12)
	assert.True(t, parseLenPrefix([]byte{0xff, 0xff, 0xff, 0xff}) == 0xffffffff)
}

func BenchmarkIterator(b *testing.B) {
	var store = newTestStore(b)
	defer store.Destroy()

	for i := int64(0); i != 1000; i++ {
		var key, val = []byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(i, 16))
		assert.NoError(b, store.DB.Put(store.WriteOptions, key, val))
	}
	var arena = make([]byte, 32*1024)

	b.Run("direct-iterator", func(b *testing.B) {
		for i := 0; i != b.N; i++ {
			var sum int
			var it = store.DB.NewIterator(store.ReadOptions)

			for it.SeekToFirst(); it.Valid(); it.Next() {
				sum += len(it.Key().Data())
				sum += len(it.Value().Data())
			}
			it.Close()
			assert.Equal(b, 5618, sum)
		}
	})
	b.Run("arena-iterator", func(b *testing.B) {
		for i := 0; i != b.N; i++ {
			var sum int
			var it = AsArenaIterator(store.DB.NewIterator(store.ReadOptions), arena)

			for it.SeekToFirst(); it.Valid(); it.Next() {
				sum += len(it.Key())
				sum += len(it.Value())
			}
			it.Close()
			assert.Equal(b, 5618, sum)
		}
	})
}

func newTestStore(t assert.TestingT) *Store {
	var dir, err = ioutil.TempDir("", "rocksdb")
	assert.NoError(t, err)

	var store = NewStore(&recoverylog.Recorder{Dir: dir})
	// Replace observed Env with regular one.
	store.Env = gorocksdb.NewDefaultEnv()
	assert.NoError(t, store.Open())

	return store
}
