package data

/*import (
	"fmt"
	"log"
)*/

/*
  Represents the status of a single machine in the group
*/

type GroupMember struct {
	Id                  int
	Address             string
	Heartbeat, Movement int
}

// Initialize a new group member
func NewGroupMember(machineId int, address string, heartBeat int, Movement int) (member *GroupMember) {
	member = new(GroupMember)
	member.Id = machineId
	member.Address = address
	member.Heartbeat = heartBeat
	member.Movement = Movement
	return
}

func (self *GroupMember) IncrementHeartBeat() {
	//log.Println("INFO", fmt.Sprintf("Heartbeat of %s: %d", self.Address, self.Heartbeat))
	self.Heartbeat++
}

func (self *GroupMember) SetHeartBeat(heartbeat int) {
	//log.Println("INFO",fmt.Sprintf("Heartbeat of %s: %d --> %d", self.Address, self.Heartbeat, heartbeat))
	self.Heartbeat = heartbeat
}
