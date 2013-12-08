//
// Created by Yaz Saito on 06/10/12.
//

// A red-black tree with an API similar to C++ STL's.
//
// The implementation is inspired (read: stolen) from:
// http://en.literateprograms.org/Red-black_tree_(C)#chunk use:private function prototypes.
//
package rbtree

//
// Public definitions
//

// Item is the object stored in each tree Node.
type Item interface{}

// Return the number of elements in the tree.
func (Root *Tree) Len() int {
	return Root.Count
}

// A convenience function for finding an element equal to key. Return
// nil if not found.
func (Root *Tree) Get(key Item) Item {
	n, exact := Root.findGE(key)
	if exact {
		return n.Item
	}
	return nil
}

// Create an iterator that points to the minimum Item in the tree
// If the tree is empty, return Limit()
func (Root *Tree) Min() Iterator {
	return Iterator{Root, Root.MinNode}
}

// Create an iterator that points at the maximum Item in the tree
//
// If the tree is empty, return NegativeLimit()
func (Root *Tree) Max() Iterator {
	if Root.MaxNode == nil {
		// TODO: there are a few checks of this form.
		// Perhaps set MaxNode=negativeLimit when the tree is empty
		return Iterator{Root, negativeLimitNode}
	}
	return Iterator{Root, Root.MaxNode}
}

// Create an iterator that points beyond the maximum Item in the tree
func (Root *Tree) Limit() Iterator {
	return Iterator{Root, nil}
}

// Create an iterator that points before the minimum Item in the tree
func (Root *Tree) NegativeLimit() Iterator {
	return Iterator{Root, negativeLimitNode}
}

// Find the smallest element N such that N >= key, and return the
// iterator pointing to the element. If no such element is found,
// return Root.Limit().
func (Root *Tree) FindGE(key Item) Iterator {
	n, _ := Root.findGE(key)
	return Iterator{Root, n}
}

// Find the largest element N such that N <= key, and return the
// iterator pointing to the element. If no such element is found,
// return iter.NegativeLimit().
func (Root *Tree) FindLE(key Item) Iterator {
	n, exact := Root.findGE(key)
	if exact {
		return Iterator{Root, n}
	}
	if n != nil {
		return Iterator{Root, n.doPrev()}
	}
	if Root.MaxNode == nil {
		return Iterator{Root, negativeLimitNode}
	}
	return Iterator{Root, Root.MaxNode}
}

// Insert an Item. If the Item is already in the tree, do nothing and
// return false. Else return true.
func (Root *Tree) Insert(Item Item) bool {
	// TODO: delay creating n until it is found to be inserted
	n := Root.doInsert(Item)
	if n == nil {
		return false
	}

	n.Color = red

	for true {
		// Case 1: N is at the Root
		if n.Parent == nil {
			n.Color = black
			break
		}

		// Case 2: The Parent is black, so the tree already
		// satisfies the RB properties
		if n.Parent.Color == black {
			break
		}

		// Case 3: Parent and uncle are both red.
		// Then paint both black and make grandParent red.
		grandParent := n.Parent.Parent
		var uncle *Node
		if n.Parent.isLeftChild() {
			uncle = grandParent.Right
		} else {
			uncle = grandParent.Left
		}
		if uncle != nil && uncle.Color == red {
			n.Parent.Color = black
			uncle.Color = black
			grandParent.Color = red
			n = grandParent
			continue
		}

		// Case 4: Parent is red, uncle is black (1)
		if n.isRightChild() && n.Parent.isLeftChild() {
			Root.rotateLeft(n.Parent)
			n = n.Left
			continue
		}
		if n.isLeftChild() && n.Parent.isRightChild() {
			Root.rotateRight(n.Parent)
			n = n.Right
			continue
		}

		// Case 5: Parent is read, uncle is black (2)
		n.Parent.Color = black
		grandParent.Color = red
		if n.isLeftChild() {
			Root.rotateRight(grandParent)
		} else {
			Root.rotateLeft(grandParent)
		}
		break
	}
	return true
}

// Delete an Item with the given key. Return true iff the Item was
// found.
func (Root *Tree) DeleteWithKey(key Item) bool {
	n, exact := Root.findGE(key)

	iter := Iterator{Root, n}
	if iter.Node != nil && exact {
		Root.DeleteWithIterator(iter)
		return true
	}
	return false
}

// Delete the current Item.
//
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (Root *Tree) DeleteWithIterator(iter Iterator) {
	doAssert(!iter.Limit() && !iter.NegativeLimit())
	Root.doDelete(iter.Node)
}

// Iterator allows scanning tree elements in sort order.
//
// Iterator invalidation rule is the same as C++ std::map<>'s. That
// is, if you delete the element that an iterator points to, the
// iterator becomes invalid. For other operation types, the iterator
// remains valid.
type Iterator struct {
	Root *Tree
	Node *Node
}

func (iter Iterator) Equal(iter2 Iterator) bool {
	return iter.Node == iter2.Node
}

// Check if the iterator points beyond the max element in the tree
func (iter Iterator) Limit() bool {
	return iter.Node == nil
}

// Check if the iterator points to the minimum element in the tree
func (iter Iterator) Min() bool {
	return iter.Node == iter.Root.MinNode
}

// Check if the iterator points to the maximum element in the tree
func (iter Iterator) Max() bool {
	return iter.Node == iter.Root.MaxNode
}

// Check if the iterator points before the minumum element in the tree
func (iter Iterator) NegativeLimit() bool {
	return iter.Node == negativeLimitNode
}

// Return the current element.
//
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (iter Iterator) Item() interface{} {
	return iter.Node.Item
}

// Create a new iterator that points to the successor of the current element.
//
// REQUIRES: !iter.Limit()
func (iter Iterator) Next() Iterator {
	doAssert(!iter.Limit())
	if iter.NegativeLimit() {
		return Iterator{iter.Root, iter.Root.MinNode}
	}
	return Iterator{iter.Root, iter.Node.doNext()}
}

// Create a new iterator that points to the predecessor of the current
// Node.
//
// REQUIRES: !iter.NegativeLimit()
func (iter Iterator) Prev() Iterator {
	doAssert(!iter.NegativeLimit())
	if !iter.Limit() {
		return Iterator{iter.Root, iter.Node.doPrev()}
	}
	if iter.Root.MaxNode == nil {
		return Iterator{iter.Root, negativeLimitNode}
	}
	return Iterator{iter.Root, iter.Root.MaxNode}
}

func doAssert(b bool) {
	if !b {
		panic("rbtree internal assertion failed")
	}
}

const red = iota
const black = 1 + iota

type Node struct {
	Item                Item
	Parent, Left, Right *Node
	Color               int // black or red
}

var negativeLimitNode *Node

//
// Internal Node attribute accessors
//
func getColor(n *Node) int {
	if n == nil {
		return black
	}
	return n.Color
}

func (n *Node) isLeftChild() bool {
	return n == n.Parent.Left
}

func (n *Node) isRightChild() bool {
	return n == n.Parent.Right
}

func (n *Node) sibling() *Node {
	doAssert(n.Parent != nil)
	if n.isLeftChild() {
		return n.Parent.Right
	}
	return n.Parent.Left
}

// Return the minimum Node that's larger than N. Return nil if no such
// Node is found.
func (n *Node) doNext() *Node {
	if n.Right != nil {
		m := n.Right
		for m.Left != nil {
			m = m.Left
		}
		return m
	}

	for n != nil {
		p := n.Parent
		if p == nil {
			return nil
		}
		if n.isLeftChild() {
			return p
		}
		n = p
	}
	return nil
}

// Return the maximum Node that's smaller than N. Return nil if no
// such Node is found.
func (n *Node) doPrev() *Node {
	if n.Left != nil {
		return maxPredecessor(n)
	}

	for n != nil {
		p := n.Parent
		if p == nil {
			break
		}
		if n.isRightChild() {
			return p
		}
		n = p
	}
	return negativeLimitNode
}

// Return the predecessor of "n".
func maxPredecessor(n *Node) *Node {
	doAssert(n.Left != nil)
	m := n.Left
	for m.Right != nil {
		m = m.Right
	}
	return m
}

//
// Tree methods
//

//
// Private methods
//

func (Root *Tree) recomputeMinNode() {
	Root.MinNode = Root.Root
	if Root.MinNode != nil {
		for Root.MinNode.Left != nil {
			Root.MinNode = Root.MinNode.Left
		}
	}
}

func (Root *Tree) recomputeMaxNode() {
	Root.MaxNode = Root.Root
	if Root.MaxNode != nil {
		for Root.MaxNode.Right != nil {
			Root.MaxNode = Root.MaxNode.Right
		}
	}
}

func (Root *Tree) maybeSetMinNode(n *Node) {
	if Root.MinNode == nil {
		Root.MinNode = n
		Root.MaxNode = n
	} else if Root.compare(n.Item, Root.MinNode.Item) < 0 {
		Root.MinNode = n
	}
}

func (Root *Tree) maybeSetMaxNode(n *Node) {
	if Root.MaxNode == nil {
		Root.MinNode = n
		Root.MaxNode = n
	} else if Root.compare(n.Item, Root.MaxNode.Item) > 0 {
		Root.MaxNode = n
	}
}

// Try inserting "Item" into the tree. Return nil if the Item is
// already in the tree. Otherwise return a new (leaf) Node.
func (Root *Tree) doInsert(Item Item) *Node {
	if Root.Root == nil {
		n := &Node{Item: Item}
		Root.Root = n
		Root.MinNode = n
		Root.MaxNode = n
		Root.Count++
		return n
	}
	Parent := Root.Root
	for true {
		comp := Root.compare(Item, Parent.Item)
		if comp == 0 {
			return nil
		} else if comp < 0 {
			if Parent.Left == nil {
				n := &Node{Item: Item, Parent: Parent}
				Parent.Left = n
				Root.Count++
				Root.maybeSetMinNode(n)
				return n
			} else {
				Parent = Parent.Left
			}
		} else {
			if Parent.Right == nil {
				n := &Node{Item: Item, Parent: Parent}
				Parent.Right = n
				Root.Count++
				Root.maybeSetMaxNode(n)
				return n
			} else {
				Parent = Parent.Right
			}
		}
	}
	panic("should not reach here")
}

// Find a Node whose Item >= key. The 2nd return value is true iff the
// Node.Item==key. Returns (nil, false) if all Nodes in the tree are <
// key.
func (Root *Tree) findGE(key Item) (*Node, bool) {
	n := Root.Root
	for true {
		if n == nil {
			return nil, false
		}
		comp := Root.compare(key, n.Item)
		if comp == 0 {
			return n, true
		} else if comp < 0 {
			if n.Left != nil {
				n = n.Left
			} else {
				return n, false
			}
		} else {
			if n.Right != nil {
				n = n.Right
			} else {
				succ := n.doNext()
				if succ == nil {
					return nil, false
				} else {
					comp = Root.compare(key, succ.Item)
					return succ, (comp == 0)
				}
			}
		}
	}
	panic("should not reach here")
}

// Delete N from the tree.
func (Root *Tree) doDelete(n *Node) {
	if n.Left != nil && n.Right != nil {
		pred := maxPredecessor(n)
		Root.swapNodes(n, pred)
	}

	doAssert(n.Left == nil || n.Right == nil)
	child := n.Right
	if child == nil {
		child = n.Left
	}
	if n.Color == black {
		n.Color = getColor(child)
		Root.deleteCase1(n)
	}
	Root.replaceNode(n, child)
	if n.Parent == nil && child != nil {
		child.Color = black
	}
	Root.Count--
	if Root.Count == 0 {
		Root.MinNode = nil
		Root.MaxNode = nil
	} else {
		if Root.MinNode == n {
			Root.recomputeMinNode()
		}
		if Root.MaxNode == n {
			Root.recomputeMaxNode()
		}
	}
}

// Move n to the pred's place, and vice versa
//
// TODO: this code is overly convoluted
func (Root *Tree) swapNodes(n, pred *Node) {
	doAssert(pred != n)
	isLeft := pred.isLeftChild()
	tmp := *pred
	Root.replaceNode(n, pred)
	pred.Color = n.Color

	if tmp.Parent == n {
		// swap the positions of n and pred
		if isLeft {
			pred.Left = n
			pred.Right = n.Right
			if pred.Right != nil {
				pred.Right.Parent = pred
			}
		} else {
			pred.Left = n.Left
			if pred.Left != nil {
				pred.Left.Parent = pred
			}
			pred.Right = n
		}
		n.Item = tmp.Item
		n.Parent = pred

		n.Left = tmp.Left
		if n.Left != nil {
			n.Left.Parent = n
		}
		n.Right = tmp.Right
		if n.Right != nil {
			n.Right.Parent = n
		}
	} else {
		pred.Left = n.Left
		if pred.Left != nil {
			pred.Left.Parent = pred
		}
		pred.Right = n.Right
		if pred.Right != nil {
			pred.Right.Parent = pred
		}
		if isLeft {
			tmp.Parent.Left = n
		} else {
			tmp.Parent.Right = n
		}
		n.Item = tmp.Item
		n.Parent = tmp.Parent
		n.Left = tmp.Left
		if n.Left != nil {
			n.Left.Parent = n
		}
		n.Right = tmp.Right
		if n.Right != nil {
			n.Right.Parent = n
		}
	}
	n.Color = tmp.Color
}

func (Root *Tree) deleteCase1(n *Node) {
	for true {
		if n.Parent != nil {
			if getColor(n.sibling()) == red {
				n.Parent.Color = red
				n.sibling().Color = black
				if n == n.Parent.Left {
					Root.rotateLeft(n.Parent)
				} else {
					Root.rotateRight(n.Parent)
				}
			}
			if getColor(n.Parent) == black &&
				getColor(n.sibling()) == black &&
				getColor(n.sibling().Left) == black &&
				getColor(n.sibling().Right) == black {
				n.sibling().Color = red
				n = n.Parent
				continue
			} else {
				// case 4
				if getColor(n.Parent) == red &&
					getColor(n.sibling()) == black &&
					getColor(n.sibling().Left) == black &&
					getColor(n.sibling().Right) == black {
					n.sibling().Color = red
					n.Parent.Color = black
				} else {
					Root.deleteCase5(n)
				}
			}
		}
		break
	}
}

func (Root *Tree) deleteCase5(n *Node) {
	if n == n.Parent.Left &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().Left) == red &&
		getColor(n.sibling().Right) == black {
		n.sibling().Color = red
		n.sibling().Left.Color = black
		Root.rotateRight(n.sibling())
	} else if n == n.Parent.Right &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().Right) == red &&
		getColor(n.sibling().Left) == black {
		n.sibling().Color = red
		n.sibling().Right.Color = black
		Root.rotateLeft(n.sibling())
	}

	// case 6
	n.sibling().Color = getColor(n.Parent)
	n.Parent.Color = black
	if n == n.Parent.Left {
		doAssert(getColor(n.sibling().Right) == red)
		n.sibling().Right.Color = black
		Root.rotateLeft(n.Parent)
	} else {
		doAssert(getColor(n.sibling().Left) == red)
		n.sibling().Left.Color = black
		Root.rotateRight(n.Parent)
	}
}

func (Root *Tree) replaceNode(oldn, newn *Node) {
	if oldn.Parent == nil {
		Root.Root = newn
	} else {
		if oldn == oldn.Parent.Left {
			oldn.Parent.Left = newn
		} else {
			oldn.Parent.Right = newn
		}
	}
	if newn != nil {
		newn.Parent = oldn.Parent
	}
}

/*
    X		     Y
  A   Y	    =>     X   C
     B C 	  A B
*/
func (Root *Tree) rotateLeft(x *Node) {
	y := x.Right
	x.Right = y.Left
	if y.Left != nil {
		y.Left.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		Root.Root = y
	} else {
		if x.isLeftChild() {
			x.Parent.Left = y
		} else {
			x.Parent.Right = y
		}
	}
	y.Left = x
	x.Parent = y
}

/*
     Y           X
   X   C  =>   A   Y
  A B             B C
*/
func (Root *Tree) rotateRight(y *Node) {
	x := y.Left

	// Move "B"
	y.Left = x.Right
	if x.Right != nil {
		x.Right.Parent = y
	}

	x.Parent = y.Parent
	if y.Parent == nil {
		Root.Root = x
	} else {
		if y.isLeftChild() {
			y.Parent.Left = x
		} else {
			y.Parent.Right = x
		}
	}
	x.Right = y
	y.Parent = x
}

func init() {
	negativeLimitNode = &Node{}
}
