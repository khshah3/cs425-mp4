package main

import (
	"fmt"
	"./rbtree"
	"reflect"
)

type MyItem struct {
	key   int
	value string
}

func (av MyItem) Dump() {
	fmt.Println("WTF")
}
func main() {
	tree := rbtree.NewTree(func(a, b rbtree.Item) int { return a.(MyItem).key - b.(MyItem).key })

	tree.Insert(MyItem{10, "value10"})
	tree.Insert(MyItem{12, "value12"})

	fmt.Println("Get(10) ->", tree.Get(MyItem{10, ""}))
	fmt.Println("Get(11) ->", tree.Get(MyItem{11, ""}))
	itemi := tree.Get(MyItem{10, ""})
	fmt.Println(itemi.(MyItem).key)

	// Find an element >= 11
	iter := tree.FindGE(MyItem{11, ""})
	//item.Dump()
	fmt.Println("FindGE(11) ->", iter.Item())
	a := reflect.ValueOf(iter.Item())
	fmt.Println(a.FieldByName("key"))

	// Find an element >= 13
	iter = tree.FindGE(MyItem{13, ""})

	// Output:
	// Get(10) -> {10 value10}
	// Get(11) -> <nil>
	// FindGE(11) -> {12 value12}
}
