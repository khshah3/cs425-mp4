package rbtree

// CompareFunc returns 0 if a==b, <0 if a<b, >0 if a>b.
type CompareFunc func(a, b Item) int

type Tree struct {
	// Root of the tree
	Root *Node

	// The minimum and maximum Nodes under the Root.
	MinNode, MaxNode *Node

	// Number of Nodes under Root, including the Root
	Count   int
	compare CompareFunc
}

// Create a new empty tree.
func NewTree(Compare CompareFunc) *Tree {
	return &Tree{compare: Compare}
}
