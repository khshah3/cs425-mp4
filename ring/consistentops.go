package ring

import (
	"../data"
	"fmt"
	"net"
  "strconv"
)

const (
	One int = iota
	Quorum
	All
)

/* Insert */
func (self *Ring) SendDataConsistent(request *data.ConsistentOpArgs, response *RpcResult) error {


	consistency := request.Consistency
	sentData := request.DataStore


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
			//fmt.Println("Cannot store data: Should not happen unless machine gone")
      // TODO
		} else {
			switch consistency {
			case One:
				response.Success = self.writeToOneReplica(sentData, self.Usertable[myAddr].Id)
			case Quorum:
				response.Success = self.writeToQuorumReplicas(sentData, self.Usertable[myAddr].Id)
			case All:
				response.Success = self.writeToReplicas(sentData, self.Usertable[myAddr].Id)
			}
      self.CmdLog.AddWrite(strconv.Itoa(consistency) + " : " + strconv.Itoa(sentData.Key) + " --> " + sentData.Value)

		}
	}

	return nil
}

/* Remove */
func (self *Ring) RemoveDataConsistent(request *data.ConsistentOpArgs, response *RpcResult) error {

	consistency := request.Consistency
	args := request.DataStore

	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{(*args).Key, ""})
	response.Success = Btoi(deleted)
	response.Member = nil
	myAddr := net.JoinHostPort(self.Address, self.Port)

	if response.Success != 1 {
		machineAddr := self.getMachineForKey(args.Key).Value
		myAddr := net.JoinHostPort(self.Address, self.Port)
		if machineAddr != myAddr {
			response.Member = self.Usertable[machineAddr]
		} else {
			fmt.Println("Data doesnt exist")
		}
	} else {
		(*args).Value = "##DELETE##"
		switch consistency {
		case One:
			//return nil
			response.Success = self.writeToOneReplica(args, self.Usertable[myAddr].Id)
		case Quorum:
			response.Success = self.writeToQuorumReplicas(args, self.Usertable[myAddr].Id)
		case All:
			response.Success = self.writeToReplicas(args, self.Usertable[myAddr].Id)
		}

	}
	return nil
}

/* Lookup */
func (self *Ring) GetDataConsistent(request *data.ConsistentOpArgs, response *RpcResult) error {

	//	consistency := request.Consistency
	args := request.DataStore


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

    cons := strconv.Itoa(request.Consistency)
    self.CmdLog.AddRead(cons + " : " + strconv.Itoa(response.Data.Key) + " --> " + response.Data.Value)
	}
	return nil
}

/* Update : Delete the current data, then add the new */
func (self *Ring) UpdateDataConsistent(request *data.ConsistentOpArgs, response *RpcResult) error {

	consistency := request.Consistency
	sentData := request.DataStore

	deleted := self.KeyValTable.DeleteWithKey(data.DataStore{(*sentData).Key, ""})
	myAddr := net.JoinHostPort(self.Address, self.Port)
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
		} else {
			switch consistency {
			case One:
				response.Success = self.writeToOneReplica(sentData, self.Usertable[myAddr].Id)
			case Quorum:
				response.Success = self.writeToQuorumReplicas(sentData, self.Usertable[myAddr].Id)
			case All:
				response.Success = self.writeToReplicas(sentData, self.Usertable[myAddr].Id)
			}
		}
	}

	return nil
}
