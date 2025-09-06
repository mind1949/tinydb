package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var errInvalidBNode = errors.New("invalid b+tree's node type")

// HEADER a fixed-size HEADER
//
// contains:
//  1. The type of node (leaf or internal)
//  2. The number of keys
const HEADER = 4

const BTREE_PAGE_SIZE uint16 = 4 * 1024 // euquals to typical OS page size
const BTREE_MAX_KEY_SIZE uint16 = 1000
const BTREE_MAX_VAL_SIZE uint16 = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE) // maximum KV
}

func assert(result bool) {
	if !result {
		panic("")
	}
}

// BNode btree node
//
// a node is just a chunk of bytes interpreted by this format.
// Moving data from memory to disk is simpler without a serialization step.
//
// a node includes:
//  1. A fixed-size header, which contains:
//     - The type of node (leaf or internal).
//     - The number of keys.
//  2. A list of pointers to child nodes for internal nodes.
//  3. A list of KV pairs.
//  4. A list of offsets to KVs, which can be used to binary search KVs
//
// this is the format of node
//
//	| type | nkeys |  pointers  |  offsets   | key-values | unused |
//	|  2B  |   2B  | nkeys × 8B | nkeys × 2B |     ...    |        |
//
// This is the format of each KV pair. Lengths followed by data.
//
//	| key_size | val_size | key | val |
//	|    2B    |    2B    | ... | ... |
//
// For example, a leaf node {"k1":"hi", "k3":"hello"} is encoded as:
//
//	| type | nkeys | pointers | offsets |            key-values           | unused |
//	|   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
//	|  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |
type BNode []byte // can be dumped to the disk

const (
	BNODE_NODE = 1 // internal ndes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

// btype btree-node's type
func (n BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(n[0:2])
}

// nkeys btree-node's keys number
func (n BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(n[2:4])
}

// setHeader
func (n BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(n[0:2], btype)
	binary.LittleEndian.PutUint16(n[2:4], nkeys)
}

// pointers btree-node's child pointers
func (n BNode) getPtr(idx uint16) uint64 {
	assert(idx < n.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(n[pos:])
}

// setPtr set btree-node's child pointer
func (n BNode) setPtr(idx uint16, val uint64) {
	assert(idx < n.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(n[pos:], val)
}

// offsetPos offset list
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (n BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(n[offsetPos(n, idx):])
}

func (n BNode) setOffset(idx uint16, offset uint16) {
	assert(idx <= n.nkeys())
	pos := HEADER + 8*idx + 2*(idx-1)
	binary.LittleEndian.PutUint16(n[pos:], offset)
}

// kvPos returns the position of the nth KV pair relative to the whole node.
func (n BNode) kvPos(idx uint16) uint16 {
	assert(idx <= n.nkeys())
	return HEADER + 8*n.nkeys() + 2*n.nkeys() + n.getOffset(idx)
}

// getKey get btree-node's idx's key
func (n BNode) getKey(idx uint16) []byte {
	assert(idx < n.nkeys())
	pos := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n[pos:])
	return n[pos+2+2:][:klen]
}

// getVal get btree-node's idx's value
func (n BNode) getVal(idx uint16) []byte {
	assert(idx < n.nkeys())
	pos := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n[pos:])
	vlen := binary.LittleEndian.Uint16(n[pos+2:])
	return n[pos+2+2+klen:][:vlen]
}

// nbytes btree-node's size in bytes
func (n BNode) nbytes() uint16 {
	return n.kvPos(n.nkeys())
}

// nodeAppendKV
//
// params:
//
//   - idx is the position of the item (a key, a value or a pointer).
//   - ptr is the nth child pointer, which is unused for leaf nodes.
//   - key and val is the KV pair. Use an empty value for internal nodes.
//
// assumes that keys are added in order.
// It uses the previous value of the offsets array to determine where the KV should be.
func nodeAppendKV(node BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	node.setPtr(idx, ptr)
	// KVs
	pos := node.kvPos(idx)
	// 4-byts KV size
	binary.LittleEndian.PutUint16(node[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	// KV data
	copy(node[pos+4:], key)
	copy(node[pos+4+uint16(len(key)):], val)
	// update the offset value for the next key
	node.setOffset(idx+1, node.getOffset(idx+1)+2+2+uint16(len(key)+len(val)))
}

// nodeAppendRange copy multiple keys, values, and pointers into the position
func nodeAppendRange(
	new, old BNode,
	dstNew, srcOld, n uint16,
) {
	for range n {
		dst, src := dstNew+1, srcOld+1
		nodeAppendKV(new, dst,
			old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

// leafInsert insert a new key-value pair at position idx with copy-on-write
func leafInsert(new, old BNode, idx uint16, key, val []byte) {
	new.setHeader(old.btype(), old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// leafUpdate update existing key
func leafUpdate(
	new, old BNode, idx uint16, key, val []byte,
) {
	new.setHeader(old.btype(), old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// nodeLookupLE find the last postion that is less than or equal to the key
// in order to find the insert position
//
// TODO: This can be a binary search instead of a linear search, since getKey(i) is O(1) with the offsets array.
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = range nkeys {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

// nodeSplit2 Split an oversized node into 2 nodes. The 2nd node always fits.
func nodeSplit2(left, right, old BNode) {
	assert(old.nkeys() >= 2)
	// the initial guess
	nleft := old.nkeys() / 2
	// try to fit the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)
	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4 // FIXME: 这里为什么要加 4
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nbytes())
	nright := old.nkeys() - nleft

	// new nodes
	left.setHeader(old.nbytes(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// NOTE: the left half may be still too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// nodeSplit3 split a node if it's too big. the results are 1~3 nodes.
//
// After splitting, the left half may still be too large, because while fitting the right half, the left half size grows. This can happen if there is a big key in the middle, requiring another split.
//
// [foo...|BIG|bar...] -> [foo...|BIG] + [bar...] -> [foo...] + [BIG] + [bar...]
//
//	too big
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// leafDelete remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(old.btype(), old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// nodeMerge merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// nodeReplace2Kid replace 2 adjacent links with 1
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}
