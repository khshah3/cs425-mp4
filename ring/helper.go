package ring

import (
	"../data"
	"../rbtree"
	"fmt"
	"math/rand"
)

//Get a random member from the table
func (self *Ring) getRandomMember() *data.GroupMember {

	tableLength := self.UserKeyTable.Len()

	receiverIndex := rand.Int() % tableLength

	//Arbitrary
	start := self.UserKeyTable.Min()

	var receiver *data.GroupMember
	var receiverAddrItem rbtree.Item
	receiverAddrItem = nil

	for i := 0; i < tableLength; i++ {
		if receiverIndex == i {
			receiverAddrItem = start.Item()
			break
		}
		start = start.Next()
	}
	if receiverAddrItem != nil {
		receiverAddress := receiverAddrItem.(data.LocationStore).Value
		receiver = self.Usertable[receiverAddress]
	} else {
		fmt.Println("You are doomed")
	}
	return receiver
}

//Get given members predecessor Key
func (self *Ring) getPredecessorKey(key int) int {
	return self.getPredecessor(key).Key
}

//Get given members successor key
func (self *Ring) getSuccessorKey(key int) int {
	return self.getSuccessor(key).Key
}

//Gets the successor
func (self *Ring) getSuccessor(key int) data.LocationStore {
	//Find successor
	successorItem := self.UserKeyTable.FindGE(data.LocationStore{key + 1, ""})
	me := self.UserKeyTable.FindLE(data.LocationStore{key, ""})

	//If reached the limit because we could not find it
	overFlow := self.UserKeyTable.Limit()

	if successorItem == overFlow {

		//Ensure that successor is not the max limit itself
		if me.Max() {

			//Then make min value successor
			successorItem = self.UserKeyTable.Min()
		} else {
			return data.NilLocationStore()
		}
	}

	successor := successorItem.Item().(data.LocationStore)
	return successor

}

//Gets the predecessor
func (self *Ring) getPredecessor(key int) data.LocationStore {

	fmt.Println(key)
	//Find predecessor
	item := self.UserKeyTable.FindLE(data.LocationStore{key - 1, ""})
	me := self.UserKeyTable.FindLE(data.LocationStore{key, ""})

	//If we could not find it we must be at the lower limit of ring
	if item == (self.UserKeyTable.NegativeLimit()) {
		//Ensure that predecessor is not the lower limit itself
		if me.Min() {
			//Then make the max value in ring predecessor
			item = self.UserKeyTable.Max()
		} else {
			return data.NilLocationStore()
		}
	}
	predecessor := item.Item().(data.LocationStore)
	return predecessor
}
