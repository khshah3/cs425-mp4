package ring

import (
	"../data"
	"fmt"
	"log"
	"net/rpc"
)

const (
	replicaNumber = 1
)

func (self *Ring) writeToReplicas(sentData *data.DataStore, key int) int {

	var result RpcResult
	i := 0
	j := 0

	for i != replicaNumber && j < self.UserKeyTable.Len() {
		fmt.Println(i, j, self.UserKeyTable.Len())
		j++
		successorItem := self.UserKeyTable.FindGE(data.LocationStore{key + 1, ""})
		overFlow := self.UserKeyTable.Limit()
		if successorItem == overFlow {
			fmt.Println("overflow")
			successorItem = self.UserKeyTable.Min()
		}
		if successorItem != overFlow {
			item := successorItem.Item()
			key = item.(data.LocationStore).Key
			value := item.(data.LocationStore).Value
			member := self.Usertable[value]

			client, err := rpc.DialHTTP("tcp", member.Address)
			if err != nil {
				log.Fatal("dialing:", err)
			}
			defer client.Close()
			err = client.Call("Ring.WriteData", sentData, &result)

			if err != nil {
				fmt.Println("Error sending data:", err)
			} else if result.Success != 1 {
				fmt.Println("Error storing data")
			} else {
				i++
			}
		}
	}

	if i != replicaNumber {
		fmt.Println("Could not replicate to all machines")
		if i == 0 {
			fmt.Println("Could not replicate to any machine ")
		}
		return 0
	} else {
		return 1
	}
}
