package ring

import (
	"../data"
	"fmt"
	"math/rand"
	"net"
)

//Get a random member from the table -- Changed so that uses first table which is client + server whereas second table is only servers
func (self *Ring) getRandomMember() *data.GroupMember {

	tableLength := len(self.Usertable)

	receiverIndex := rand.Int() % tableLength
	var receiver *data.GroupMember

	i := 0
	for _, value := range self.Usertable {
		if receiverIndex == i {
			if value.Id != -1 {
				receiver = value
				return receiver
			} else {
				receiverIndex = (receiverIndex + 1) % tableLength
			}
		}
		i++
	}
	fmt.Println("Should only be here if there no live members")
	//Arbitrary
	/*start := self.UserKeyTable.Min()


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
	}*/
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

//Get current machines key
func (self *Ring) getKey() int {

	myAddr := net.JoinHostPort(self.Address, self.Port)
	return self.Usertable[myAddr].Id
}
