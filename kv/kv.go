// Package kv a KV store with a copy-on-write B+tree backed by a file.
package kv

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/mind1949/tinydb/btree"
	"golang.org/x/sys/unix"
)

// KV a kv store
//
// kv store is a is a single file divided into “pages”.
// Each page is a B+tree node, except for the 1st page;
// the 1st page contains the pointer to the latest root node and other auxiliary data, we call this the meta page.
//
//	|     the_meta_page    | pages... | root_node | pages... | (end_of_file)
//	| root_ptr | page_used |                ^                ^
//	      |          |                      |                |
//	      +----------|----------------------+                |
//	                 |                                       |
//	                 +---------------------------------------+
//
// New nodes are simply appended like a log,
// but we cannot use the file size to count the number of pages,
// because after a power loss the file size (metadata) may become inconsistent with the file data.
// This is filesystem dependent, we can avoid this by storing the number of pages in the meta page.
type KV struct {
	Path string // filename

	fd   int
	tree btree.BTree

	mmap struct {
		// mmap size, can be larger than the file size
		total int
		// mremap remaps a mapping to a larger range, it’s like realloc.
		// That’s one way to deal with the growing file.
		// However, the address may change, which can hinder concurrent readers in later chapters.
		// Our solution is to add new mappings to cover the expanded file.
		chunks [][]byte // multiple mmaps, can be non-continuous
	}

	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}

	failed bool // Did the last update fail?
}

// Open open or create
func (k *KV) Open() error {
	k.tree = btree.NewBTree(
		k.pageRead,   // read a page
		k.pageAppend, // apppend a page
		func(uint64) {},
	)
	// TODO:
	return nil
}

// Get get value with key
func (k *KV) Get(key []byte) ([]byte, error) {
	return k.tree.Get(key)
}

// Set set key with val
func (k *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(k) // save the in-memory state (tree root)
	err := k.tree.Insert(key, val)
	if err != nil {
		return err
	}
	return updateOrRevert(k, meta)
}

// Del delete key
func (k *KV) Del(key []byte) (bool, error) {
	deleted, err := k.tree.Delete(key)
	if err != nil {
		return false, err
	}
	return deleted, updateFile(k)
}

// pageRead `BTree.get`, read a page.
func (k *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range k.mmap.chunks {
		end := start + uint64(len(chunk))/uint64(btree.BTREE_PAGE_SIZE)
		if ptr < end {
			offset := uint64(btree.BTREE_PAGE_SIZE) * (ptr - start)
			return chunk[offset : offset+uint64(btree.BTREE_PAGE_SIZE)]
		}
		start = end
	}
	panic("bad ptr")
}

func updateOrRevert(kv *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if kv.failed {
		// write and fsync the previous meta page
		// TODO:

		kv.failed = false
	}
	// 2-phase update
	err := updateFile(kv)
	// revert on error
	if err != nil {
		// the on-disk meta page is in an unknown state;
		// mark it to be rewritten on later recovery.
		kv.failed = true
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(kv, meta)
		// discard temporaries
		kv.page.temp = kv.page.temp[:0]
	}
	return err
}

func updateFile(kv *KV) error {
	// 1. Write new nodes.
	if err := writePages(kv); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	if err := syscall.Fsync(kv.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(kv); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	return syscall.Fsync(kv.fd)
}

func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	//
	// guarantees that the file is from the same directory we opened before,
	// in case the directory path is replaced in between ([race condition](https://val.packett.cool/blog/use-openat/)).
	// This is only a minor concern. Moving a directory with a running DB in it is not a sane scenario.
	flags = os.O_RDWR | os.O_CREATE
	fd, err := unix.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the directory
	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd) // may leave an empty file
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	return fd, nil
}

// pageAppend
//
// The BTree.new callback collects new pages from B+tree updates, and allocates the page number from the end of DB.
func (k *KV) pageAppend(node []byte) uint64 {
	ptr := k.page.flushed + uint64(len(k.page.temp)) // just append
	k.page.temp = append(k.page.temp, node)
	return ptr
}

// writePages
func writePages(kv *KV) error {
	// extend the mmap if needed
	size := (int(kv.page.flushed) + len(kv.page.temp)) * int(btree.BTREE_PAGE_SIZE)
	if err := extendMmap(kv, size); err != nil {
		return err
	}
	// write data pages to the file
	offset := int64(kv.page.flushed * uint64(btree.BTREE_PAGE_SIZE))
	if _, err := unix.Pwritev(kv.fd, kv.page.temp, offset); err != nil {
		return err
	}
	// discard in-memory data
	kv.page.flushed += uint64(len(kv.page.temp))
	kv.page.temp = kv.page.temp[:0]
	return nil
}

// updateRoot Update the meta page. it must be atomic.
func updateRoot(kv *KV) error {
	if _, err := syscall.Pwrite(kv.fd, saveMeta(kv), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

func extendMmap(kv *KV, size int) error {
	if size <= kv.mmap.total {
		return nil // enough range
	}
	alloc := max(kv.mmap.total, 64<<20) // double the current address space
	for kv.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		kv.fd, int64(kv.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	kv.mmap.total += alloc
	kv.mmap.chunks = append(kv.mmap.chunks, chunk)
	return nil
}

const DB_SIG = "BuildYourOwnDB"

// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func saveMeta(kv *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], kv.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[24:], kv.page.flushed)
	return data[:]
}

func loadMeta(kv *KV, data []byte) error {
	// TODO:
	return nil
}
