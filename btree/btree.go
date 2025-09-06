// Pacakge btree copy-on-write B+tree
package btree

import (
	"bytes"
	"errors"
	"fmt"
)

// NewBTree instantiate a b+tree
func NewBTree(
	get func(uint64) []byte, // dereference a pointer
	new func([]byte) uint64, // alocate a new page
	del func(uint64), // deallocate a page
) BTree {
	return BTree{
		get: get,
		new: new,
		del: del,
	}
}

// BTree
//
// 3 invariants(to preserve when updating a B+tree):
//  1. Same height for all leaf nodes.
//  2. Node size is bounded by a constant.
//  3. Node is not empty.
type BTree struct {
	// pointer (a nonzero page number)
	root uint64

	// callbacks for managing on-diskpages
	get func(uint64) []byte // dereference a pointer
	new func([]byte) uint64 // alocate a new page
	del func(uint64)        // deallocate a page
}

// GetRoot get root node pointer
func (t *BTree) GetRoot() uint64 {
	return t.root
}

// Insert insert a new key or update an existing key
func (t *BTree) Insert(key, val []byte) error {
	// check the length limit imposed by the node format
	if err := checkLimit(key, val); err != nil {
		return err // the only way for an update to fail
	}
	// create the fist node
	if t.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		t.root = t.new(root)
		return nil
	}
	// insert the key
	node := treeInsert(t, t.get(t.root), key, val)
	// grow the tree if the root is split
	nsplit, split := nodeSplit3(node)
	t.del(t.root)
	switch nsplit {
	case 1:
		t.root = t.new(split[0])
	case 2, 3:
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := t.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		t.root = t.new(root)
	default:
		panic(fmt.Sprintf("impossible nsplit: %d", nsplit))
	}
	return nil
}

func checkLimit(key, val []byte) error {
	if err := checkLimitKey(key); err != nil {
		return err
	}
	if len(val) >= int(BTREE_MAX_VAL_SIZE) {
		return errors.New("btree's node val size cann't be max than BTREE_MAX_VAL_SIZE")
	}
	return nil
}

func checkLimitKey(key []byte) error {
	if len(key) == 0 {
		return errors.New("btree's node key cann't be zero length")
	}
	if len(key) >= int(BTREE_MAX_KEY_SIZE) {
		return errors.New("btree's node key size cann't be max than BTREE_MAX_KEY_SIZE")
	}
	return nil
}

// treeInsert
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}
	case BNODE_NODE: // internal node, walk into the child node
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)
		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)
		// deallocate the old kid node
		tree.del(kptr)
		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}
	return new
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// ErrKeyNotFound cann't find key from b+tree
var ErrKeyNotFound = errors.New("key not found in b+tree")

// Delete delete a key and returns whether the key was there
func (t *BTree) Delete(key []byte) (bool, error) {
	if err := checkLimitKey(key); err != nil {
		return false, err
	}
	if t.root == 0 {
		return false, errors.New("btree's root cann't be zero")
	}

	updated, err := treeDelete(t, t.get(t.root), key)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	t.del(t.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		t.root = updated.getPtr(0)
	} else {
		t.root = t.new(updated)
	}
	return true, nil
}

// treeDelete delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) (BNode, error) {
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}, ErrKeyNotFound
		}

		// The extra size allows it to exceed 1 page temporarily.
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, idx) // found, delete it
		return new, nil
	case BNODE_NODE: // internal node, walk into the child node
		return nodeDelete(tree, node, idx, key)
	default:
		return BNode{}, errInvalidBNode
	}
}

// nodeDelete delete a key from an internal node; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) (BNode, error) {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated, err := treeDelete(tree, tree.get(kptr), key)
	if err != nil {
		return updated, err
	}
	tree.del(kptr)
	// check for merging
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)          // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new, nil
}

// shouldMerge should the updted kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode, idx uint16, updated BNode,
) (direction int, sibling BNode) {
	// Deleted keys mean unused space within nodes.
	// In the worst case, a mostly empty tree can still retain a large number of nodes.
	// We can improve this by triggering merges earlier —
	// using 1/4 of a page as a threshold instead of the empty node,
	// which is an arbitrary soft limit on the minimum node size.
	const MERGE_THRESHOLD = BTREE_PAGE_SIZE / 4
	if updated.nbytes() > MERGE_THRESHOLD {
		return 0, BNode{}
	}

	if idx > 0 {
		leftSibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := leftSibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_MAX_KEY_SIZE {
			return -1, leftSibling
		}
	}
	if idx+1 < node.nkeys() {
		rightSibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := rightSibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, rightSibling
		}
	}
	return 0, BNode{}
}

// Get get value with key
func (t *BTree) Get(key []byte) ([]byte, error) {
	if err := checkLimitKey(key); err != nil {
		return nil, err
	}
	if t.root == 0 {
		return nil, ErrKeyNotFound
	}

	node := BNode(t.get(t.root))
	for node.btype() == BNODE_NODE {
		idx := nodeLookupLE(node, key)
		node = BNode(t.get(node.getPtr(idx)))
	}
	idx := nodeLookupLE(node, key)
	if idx >= node.nkeys() || !bytes.Equal(key, node.getKey(idx)) {
		return nil, ErrKeyNotFound
	}
	return node.getVal(idx), nil
}
