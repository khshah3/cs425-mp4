package ring

import (
	"../data"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (self *Ring) createTCPListener(hostPort string) {
	var tcpaddr *net.TCPAddr
	tcpaddr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return
	}
	rpc.Register(self)
	rpc.HandleHTTP()

	conn, err := net.ListenTCP("tcp", tcpaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(conn, nil)
}

/*
   The following are the calls exposed over RPC and may be called from a remote machine
   They correspont to the Insert/Update/Remove/Lookup calls on the client
*/

/* Insert */
func (self *Ring) SendData(sentData *data.DataStore, response *RpcResult) error {

	//Check if there is a newer machine for this
	machineAddr := self.getMachineForKey(sentData.Key).Value
	myAddr := net.JoinHostPort(self.Address, self.Port)

	//newer machine exists
	if machineAddr != myAddr {
		response.Member = self.Usertable[machineAddr]
		//i am the newest
	} else {
		inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
		response.Success = Btoi(inserted)
		if response.Success != 1 {
			fmt.Println("Cannot store date: Should not happen unless machine gone")
		}
	}

	return nil
}

/* Remove */
func (self *Ring) RemoveData(args *data.DataStore, response *RpcResult) error {
	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{(*args).Key, ""})
	response.Success = Btoi(deleted)
	response.Member = nil
	if response.Success != 1 {
		machineAddr := self.getMachineForKey(args.Key).Value
		myAddr := net.JoinHostPort(self.Address, self.Port)
		if machineAddr != myAddr {
			response.Member = self.Usertable[machineAddr]
		} else {
			fmt.Println("Data doesnt exist")
		}

	}
	return nil
}

/* Lookup */
func (self *Ring) GetData(args *data.DataStore, response *RpcResult) error {
	found := self.KeyValTable.Get(data.DataStore{(*args).Key, ""})
	response.Member = nil
	if found == nil {
		machineAddr := self.getMachineForKey(args.Key).Value
		myAddr := net.JoinHostPort(self.Address, self.Port)
		if machineAddr != myAddr {
			response.Member = self.Usertable[machineAddr]
		} else {
			fmt.Println("Data doesnt exist")
		}
		response.Success = 0
	} else {
		response.Success = 1
		response.Data = found.(data.DataStore)
	}
	return nil
}

/* Update : Delete the current data, then add the new */
func (self *Ring) UpdateData(sentData *data.DataStore, response *RpcResult) error {
	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{(*sentData).Key, ""})
	if !deleted {
		machineAddr := self.getMachineForKey(sentData.Key).Value
		myAddr := net.JoinHostPort(self.Address, self.Port)
		if machineAddr != myAddr {
			response.Member = self.Usertable[machineAddr]
		} else {
			fmt.Println("Data doesnt exist")
		}
		response.Success = 0
	} else {
		inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
		response.Success = Btoi(inserted)
		if response.Success != 1 {
			fmt.Println("Cannot update date: Should not happen unless machine gone")
		}
	}

	return nil
}

//Get data for when joining the group
func (self *Ring) GetEntryData(location *data.LocationStore, responseData *data.DataStore) error {

	key := (*location).Key
	mdata := &data.DataStore{
		Key:   -1,
		Value: "",
	}

	*responseData = *mdata

	False := self.KeyValTable.Limit()
	min := self.KeyValTable.Min()

	if min != False {
		if min.Item().(data.DataStore).Key <= key {
			*responseData = min.Item().(data.DataStore)
			member := data.NewGroupMember((*responseData).Key, (*location).Value, 0, Joining)
			self.updateMember(member)
			self.KeyValTable.DeleteWithIterator(min)
		}
	}
	return nil
}

func (self *Ring) SendLeaveData(sentData *data.DataStore, response *RpcResult) error {

	inserted := self.KeyValTable.Insert(data.DataStore{(*sentData).Key, (*sentData).Value})
	response.Success = Btoi(inserted)
	if response.Success != 1 {
		fmt.Println("Cannot store date: Should not happen unless machine gone")
	}
	return nil
}

/*
  Some other utility functions that may be called over RPC
*/
func (self *Ring) GetSuccessor(key *int, currSuccessorMember **data.GroupMember) error {

	start := self.UserKeyTable.Min()
	for i := 0; i < self.UserKeyTable.Len(); i++ {
		value := start.Item().(data.LocationStore).Value
		fmt.Println(self.Usertable[value])
		start = start.Next()
	}

	successorItem := self.UserKeyTable.FindGE(data.LocationStore{*key + 1, ""})
	overFlow := self.UserKeyTable.Limit()
	fmt.Println(successorItem)
	if successorItem == overFlow {
		fmt.Println("overflow")
		successorItem = self.UserKeyTable.Min()
	}
	if successorItem != self.UserKeyTable.Limit() {
		fmt.Println("IGetting")
		item := successorItem.Item()
		value := item.(data.LocationStore).Value
		member := self.Usertable[value]
		fmt.Println(member.Id)
		*currSuccessorMember = member
		//We can add code to update member key here as well? Or we can wait for it to be gossiped to us

	} else {
		*currSuccessorMember = nil
	}
	return nil
}

// Utility bool-to-int conversion
func Btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
