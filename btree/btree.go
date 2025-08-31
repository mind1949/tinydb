package btree

import (
	"bytes"
)

// BTree
//
// 3 invariants:
// > to preserve when updating a B+tree
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
