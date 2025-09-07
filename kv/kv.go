// Package kv a KV store with a copy-on-write B+tree backed by a file.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/mind1949/tinydb/btree"
	"github.com/mind1949/tinydb/meta"
	"golang.org/x/sys/unix"
)

// KV a kv store
//
// kv store is a is a single file divided into “pages”.
// Each page is a B+tree node, except for the 1st page;
// the 1st page contains the pointer to the latest root node and other auxiliary data, we call this the meta page.
//
//	|      the_meta_page         | pages... | root_node | pages... | (end_of_file)
//	| sig | root_ptr | page_used |                ^                ^
//	            |          |                      |                |
//	            +----------|----------------------+                |
//	                       |                                       |
//	                       +---------------------------------------+
//
// New nodes are simply appended like a log,
// but we cannot use the file size to count the number of pages,
// because after a power loss the file size (metadata) may become inconsistent with the file data.
// This is filesystem dependent, we can avoid this by storing the number of pages in the meta page.
type KV struct {
	// filename
	Path string
	// filename fd
	fd int

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
	fd, err := createFileSync(k.Path)
	if err != nil {
		return err
	}
	k.fd = fd
	// TODO:
	return nil
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

// pageAppend
//
// The BTree.new callback collects new pages from B+tree updates, and allocates the page number from the end of DB.
func (k *KV) pageAppend(node []byte) uint64 {
	ptr := k.page.flushed + uint64(len(k.page.temp)) // just append
	k.page.temp = append(k.page.temp, node)
	return ptr
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
	meta := saveMeta(k) // save the in-memory state (tree root)
	deleted, err := k.tree.Delete(key)
	if err != nil {
		return false, err
	}
	return deleted, updateOrRevert(k, meta)
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
//
// Writing a small amount of page-aligned data to a real disk,
// modifying only a single sector, is likely power-loss-atomic at the hardware level.
// Some [real databases](https://www.postgresql.org/message-id/flat/17064-bb0d7904ef72add3%40postgresql.org) depend on this.
// That’s how we update the meta page too.
func updateRoot(kv *KV) error {
	if _, err := syscall.Pwrite(kv.fd, saveMeta(kv), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// extendMmap
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

const metaSig = meta.NAME + " format " + meta.VERSION

const (
	meta_SIG_SIZE        = 16
	meta_ROOT_PTR_SIZE   = 8
	meta_PAGE_USERD_SIZE = 8
)

// saveMeta
//
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func saveMeta(kv *KV) []byte {
	var data [32]byte
	copy(data[:meta_SIG_SIZE], []byte(metaSig))
	binary.LittleEndian.PutUint64(data[meta_SIG_SIZE:], kv.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[meta_SIG_SIZE+meta_ROOT_PTR_SIZE:], kv.page.flushed)
	return data[:]
}

// loadMeta
func loadMeta(kv *KV, data []byte) error {
	if err := checkMeta(data); err != nil {
		return err
	}

	root := binary.LittleEndian.Uint64(data[meta_SIG_SIZE:])
	kv.tree.SetRoot(root)
	flushed := binary.LittleEndian.Uint64(data[meta_SIG_SIZE+meta_ROOT_PTR_SIZE:])
	kv.page.flushed = flushed
	return nil
}

func checkMeta(data []byte) error {
	if len(data) < meta_SIG_SIZE+meta_ROOT_PTR_SIZE+meta_PAGE_USERD_SIZE {
		return errors.New("invalid meta length")
	}
	if err := checkMetaSig(data); err != nil {
		return err
	}
	return nil
}

func checkMetaSig(data []byte) error {
	if len(data) < meta_SIG_SIZE || !bytes.Equal(data[:meta_SIG_SIZE], []byte(metaSig)) {
		return errors.New("invalid meta sig")
	}
	return nil
}
