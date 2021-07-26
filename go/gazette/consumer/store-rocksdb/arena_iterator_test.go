package store_rocksdb

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	rocks "github.com/jgraettinger/gorocksdb"
)

func TestArenaIterator(t *testing.T) {
	var db, wo, ro, cleanup = newTestDB(t)
	defer cleanup()

	var expect = [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for _, k := range expect {
		require.NoError(t, db.Put(wo, k, []byte("val")))
	}

	for _, size := range []int{0, 2, 20, 200, 32 * 1024} {
		var it = AsArenaIterator(db.NewIterator(ro), make([]byte, size))

		var actual [][]byte
		for it.SeekToFirst(); it.Valid(); it.Next() {
			actual = append(actual, append([]byte(nil), it.Key()...))
		}
		require.NoError(t, it.Err())
		require.Equal(t, expect, actual)

		it.Close()
	}
}

func TestArenaIteratorSeeking(t *testing.T) {
	var db, wo, ro, cleanup = newTestDB(t)
	defer cleanup()

	for i := int64(100); i != 200; i++ {
		var key, val = []byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(i, 16))
		require.NoError(t, db.Put(wo, key, val))
	}

	for _, size := range []int{0, 2, 20 /*200, 32 * 1024*/} {
		var iter = AsArenaIterator(db.NewIterator(ro), make([]byte, size))

		var expect = func(i int64) {
			require.True(t, iter.Valid())
			require.Equal(t, strconv.FormatInt(i, 10), string(iter.Key()))
			require.Equal(t, strconv.FormatInt(i, 16), string(iter.Value()))
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
		require.False(t, iter.Valid())

		iter.SeekToFirst()
		expect(100)
		iter.Next()
		expect(101)
		iter.Prev()
		expect(100)

		// Step before first element.
		iter.Prev()
		require.False(t, iter.Valid())

		iter.Close()
	}
}

func TestLenPrefixParsing(t *testing.T) {
	require.True(t, parseLenPrefix([]byte{0x00, 0x00, 0x00, 0x00}) == 0x00000000)
	require.True(t, parseLenPrefix([]byte{0x00, 0x00, 0x02, 0x83}) == 0x00000283)
	require.True(t, parseLenPrefix([]byte{0x01, 0x01, 0x01, 0x01}) == 0x01010101)
	require.True(t, parseLenPrefix([]byte{0xab, 0xcd, 0xef, 0x12}) == 0xabcdef12)
	require.True(t, parseLenPrefix([]byte{0xff, 0xff, 0xff, 0xff}) == 0xffffffff)
}

func BenchmarkIterator(b *testing.B) {
	var db, wo, ro, cleanup = newTestDB(b)
	defer cleanup()

	for i := int64(0); i != 1000; i++ {
		var key, val = []byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(i, 16))
		require.NoError(b, db.Put(wo, key, val))
	}
	var arena = make([]byte, 32*1024)

	b.Run("direct-iterator", func(b *testing.B) {
		for i := 0; i != b.N; i++ {
			var sum int
			var it = db.NewIterator(ro)

			for it.SeekToFirst(); it.Valid(); it.Next() {
				sum += len(it.Key().Data())
				sum += len(it.Value().Data())
			}
			it.Close()
			require.Equal(b, 5618, sum)
		}
	})
	b.Run("arena-iterator", func(b *testing.B) {
		for i := 0; i != b.N; i++ {
			var sum int
			var it = AsArenaIterator(db.NewIterator(ro), arena)

			for it.SeekToFirst(); it.Valid(); it.Next() {
				sum += len(it.Key())
				sum += len(it.Value())
			}
			it.Close()
			require.Equal(b, 5618, sum)
		}
	})
}

func newTestDB(t require.TestingT) (*rocks.DB, *rocks.WriteOptions, *rocks.ReadOptions, func()) {
	var dir, err = ioutil.TempDir("", "rocksdb")
	require.NoError(t, err)

	var options = rocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	var ro = rocks.NewDefaultReadOptions()
	var wo = rocks.NewDefaultWriteOptions()

	db, err := rocks.OpenDb(options, dir)
	require.NoError(t, err)

	var cleanup = func() {
		db.Close()
		ro.Destroy()
		wo.Destroy()

		require.NoError(t, os.RemoveAll(dir))
	}

	return db, wo, ro, cleanup
}
