/*
  Represents the ring of machines. Hashing a machine or key to a value on the
  ring is done here.
*/

package ring

import (
	"../data"
	"../logger"
	"../rbtree"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"time"
)

const (
	Leaving = iota
	Stable
	Joining
)

const (
	heartbeatThreshold = 25
)

type Ring struct {
	Usertable    map[string]*data.GroupMember
	UserKeyTable *rbtree.Tree
	KeyValTable  *rbtree.Tree
	Port         string
	Address      string
	Heartbeats   int
	ConnUDP      *net.UDPConn
	Active       bool
	isGossiping  bool
	Successor    *data.GroupMember
}

/*
Create a new machine that joins the ring and can query the others
*/
func NewMember(hostPort string, faultTolerance int) (ring *Ring, err error) {

	log.Printf("Creating udp listener at %s\n", hostPort)
	logger.Log("INFO", "Creating udp listener at"+hostPort)
	connUDP, err := createUDPListener(hostPort)

	delim := ":"
	fields := strings.SplitN(hostPort, delim, 2)
	address, port := fields[0], fields[1]

	if err != nil {
		return
	}

	userKeyVal := rbtree.NewTree(func(a, b rbtree.Item) int { return a.(data.LocationStore).Key - b.(data.LocationStore).Key })
	keyVal := rbtree.NewTree(func(a, b rbtree.Item) int { return a.(data.DataStore).Key - b.(data.DataStore).Key })

	ring = &Ring{
		Usertable:    make(map[string]*data.GroupMember),
		UserKeyTable: userKeyVal,
		KeyValTable:  keyVal,
		Port:         port,
		Address:      address,
		Heartbeats:   faultTolerance,
		ConnUDP:      connUDP,
		Active:       true,
		isGossiping:  false,
		Successor:    nil,
	}

	log.Printf("Creating tcp listener at %s\n", hostPort)
	ring.createTCPListener(hostPort)
	fmt.Print(ring.Usertable)

	return
}

/* Returns an RPC client to the key's successor in the ring, allowing us to call functions on it */
func (self *Ring) dialSuccessor(key int) *rpc.Client {
	successorId := self.UserKeyTable.FindGE(data.LocationStore{key, ""})
	if successorId == self.UserKeyTable.Limit() {
		successorId = self.UserKeyTable.Min()
	}
	successorAddr := self.Usertable[successorId.Item().(data.LocationStore).Value].Address
	fmt.Println(successorAddr)

	client, err := rpc.DialHTTP("tcp", successorAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

/* Format of all the RPC responses for consistency */
type RpcResult struct {
	Success int
	Data    data.DataStore
	Member  *data.GroupMember
}

/* Make an RPC call to the key's successor machine, using the given args */
func (self *Ring) callSuccessorRPC(key int, function string, args *data.DataStore) (result RpcResult) {
	client := self.dialSuccessor(key)
	defer client.Close()
	err := client.Call(function, args, &result)
	if err != nil {
		fmt.Println("Error sending data:", err)
		return
	}
	if result.Success != 1 {
		fmt.Println("Error storing data")
	}
	fmt.Printf("Data Size: %d \n", self.KeyValTable.Len())
	return result
}

/* The actual Operations exposed over RPC */
func (self *Ring) Insert(key int, val string) {
	args := data.NewDataStore(key, val)
	result := self.callSuccessorRPC(key, "Ring.SendData", args)

	//Found New Member
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Insert(key, val)
	}
}

func (self *Ring) Update(key int, val string) {
	args := data.NewDataStore(key, val)
	result := self.callSuccessorRPC(key, "Ring.UpdateData", args)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Update(key, val)
	}
}

func (self *Ring) Remove(key int) {
	args := data.NewDataStore(key, "")
	result := self.callSuccessorRPC(key, "Ring.RemoveData", args)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Remove(key)
	}
}

func (self *Ring) Lookup(key int) {
	args := data.NewDataStore(key, "")
	result := self.callSuccessorRPC(key, "Ring.GetData", args)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Lookup(key)
	} else {
		fmt.Println(result.Data.Key, result.Data.Value)
	}

}

func (self *Ring) updateMember(updatedMember *data.GroupMember) {

	if updatedMember == nil {
		return
	}

	key := updatedMember.Id
	movement := updatedMember.Movement
	member := self.Usertable[updatedMember.Address]
	var lastKey int
	lastKey = -1
	if member != nil {
		lastKey = self.Usertable[updatedMember.Address].Id
	}

	//Add new member
	if member == nil {
		self.Usertable[updatedMember.Address] = updatedMember
		if self.UserKeyTable.Get(data.LocationStore{key, ""}) == nil {
			self.UserKeyTable.Insert(data.LocationStore{key, updatedMember.Address})

		} else {
			fmt.Println("ERROR: Two members with same key")
		}
		return
		//Change current member key to new one
	} else {

		if member.Heartbeat > updatedMember.Heartbeat {
			member.SetHeartBeat(0)
		}
		if member.Movement >= movement {
			self.Usertable[updatedMember.Address] = updatedMember
			if ((movement == Joining || member.Movement == Joining) && (key > lastKey)) ||
				((movement == Leaving || member.Movement == Leaving) && (key < lastKey)) {

				fmt.Printf("Deleting member with ID %d FROM %s", lastKey, updatedMember.Address)
				self.UserKeyTable.DeleteWithKey(data.LocationStore{lastKey, ""})

				if key != -1 {
					fmt.Printf("Inserting member with ID %d FROM %s", key, updatedMember.Address)
					self.UserKeyTable.Insert(data.LocationStore{key, updatedMember.Address})
				}
			}
		} else {
			fmt.Println("You should not be able to join if you already exist or stay if you already started leaving")
		}

	}
	return
}

func (self *Ring) FirstMember(portAddress string) {
	key := data.Hasher(portAddress)
	fmt.Println("Found")
	fmt.Println(key)
	newMember := data.NewGroupMember(key, portAddress, 0, Stable)
	self.updateMember(newMember)
}

func (self *Ring) getMachineForKey(key int) data.LocationStore {
	successor := self.UserKeyTable.FindGE(data.LocationStore{key, ""})
	if successor == self.UserKeyTable.Limit() {
		successor = self.UserKeyTable.Min()
	}
	return successor.Item().(data.LocationStore)
}

func (self *Ring) Gossip() {
	fmt.Println("Start Gossiping")
	self.isGossiping = true
	heartbeatInterval := 50 * time.Millisecond
	userTableInterval := 250 * time.Millisecond

	go self.HeartBeatGossip(heartbeatInterval)
	go self.UserTableGossip(userTableInterval)
}

func (self *Ring) HeartBeatGossip(interval time.Duration) {
	for {
		//self.doHeartBeatGossip()
		time.Sleep(interval)
	}

}

func (self *Ring) UserTableGossip(interval time.Duration) {
	for {
		self.doUserTableGossip()
		time.Sleep(interval)
	}

}

func (self *Ring) ReceiveDatagrams(joinGroupOnConnection bool) {
	if self.Active == false {
		return
	}
	for {
		buffer := make([]byte, 1024)
		c, addr, err := self.ConnUDP.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("%d byte datagram from %s with error %s\n", c, addr.String(), err.Error())
			logger.Log("ERROR", addr.String()+"byte datagram from %s with error "+err.Error())
			return
		}

		portmsg := strings.SplitN(string(buffer[:c]), "<PORT>", 2)
		port, msg := portmsg[0], portmsg[1]
		senderAddr := net.JoinHostPort(addr.IP.String(), port)

		logger.Log("INFO", "Data received from "+senderAddr+" : "+msg)
		self.handleMessage(msg, senderAddr, &joinGroupOnConnection)
	}
}

func (self *Ring) handleMessage(msg, sender string, joinSenderGroup *bool) {
	fields := strings.SplitN(msg, "|%|", 2)
	switch fields[0] {
	case "GOSSIP":
		logger.Log("GOSSIP", "Gossiping "+sender+fields[1])
		self.handleGossip(sender, fields[1])
	}
}

func (self *Ring) handleGossip(senderAddr, subject string) {
	// Reset the counter for the sender
	// TODO add sender if it doesn't exist yet

	subjectMember := data.Unmarshal(subject)

	if subjectMember == nil {
		return
	}

	sender := self.Usertable[senderAddr]
	if sender != nil {
		self.Usertable[senderAddr].SetHeartBeat(0)
	}
	self.updateMember(subjectMember)
	//fmt.Println("Updating Heartbeat to ", senderAddr, self.Usertable[senderAddr].Heartbeat)
}

//Join the group by finding successor and getting all the required data from it
func (self *Ring) JoinGroup(address string) (err error) {

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	//Get Successor
	hostPort := net.JoinHostPort(self.Address, self.Port)
	hashedKey := data.Hasher(hostPort + time.Now().String()) // TODO this is a hack

	successor := self.callForSuccessor(hashedKey, address)
	argi := data.NewLocationStore(hashedKey, hostPort)
	client, err = rpc.DialHTTP("tcp", successor.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println(successor)
	//Get smallest key less then key and initiate data transfer
	var data_t data.DataStore
	err = client.Call("Ring.GetEntryData", argi, &data_t)

	fmt.Printf("Transferring Data Key: %d Value: %s", data_t.Key, data_t.Value)
	for data_t.Key != -1 {

		//Insert Key into my table
		self.KeyValTable.Insert(data.DataStore{data_t.Key, data_t.Value})

		hostPort := net.JoinHostPort(self.Address, self.Port)

		//Insert value of key as my Id
		newMember := data.NewGroupMember(data_t.Key, hostPort, 0, Joining)
		self.updateMember(newMember)

		//Start Gossiping
		if self.isGossiping == false {
			go self.Gossip()
		}

		//Check if more data_t is available
		err = client.Call("Ring.GetEntryData", argi, &data_t)
		if err != nil {
			fmt.Println("Error retrieving data")
			return
		}
	}

	//Make hashed key my id
	finalMember := data.NewGroupMember(hashedKey, hostPort, 0, Stable)
	self.updateMember(finalMember)

	fmt.Println("Am i done")
	if self.isGossiping == false {
		go self.Gossip()
		fmt.Println("Am i done")
	}
	return
}

//Leave the group by transferring all data to successor
func (self *Ring) LeaveGroup() {

	//TODO: We have stored successor but he could change so lets find ask a random member
	hostPort := net.JoinHostPort(self.Address, self.Port)
	key := self.Usertable[hostPort].Id
	fmt.Println(self.Usertable[hostPort].Id)
	receiver := self.getRandomMember()
	successor := self.callForSuccessor(key, receiver.Address)
	fmt.Println(successor)

	client, err := rpc.DialHTTP("tcp", successor.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	fmt.Println(self.KeyValTable.Len())
	NextLessThen := self.KeyValTable.FindLE(data.DataStore{key, ""})
	for NextLessThen != self.KeyValTable.NegativeLimit() {

		sendData := NextLessThen.Item().(data.DataStore)
		sendDataPtr := &sendData
		var result RpcResult
		err = client.Call("Ring.SendLeaveData", sendDataPtr, &result)
		if err != nil {
			fmt.Println("Error sending data", err)
			return
		}
		fmt.Println(result)
		if result.Success == 1 {
			fmt.Println("Data Succesfully sent")
			self.KeyValTable.DeleteWithIterator(NextLessThen)
			self.updateMember(data.NewGroupMember(sendData.Key, hostPort, 0, Leaving))
		} else {
			fmt.Println("Error sending data", err)
			break
		}
		NextLessThen = self.KeyValTable.FindLE(data.DataStore{sendData.Key, ""})
	}
	self.updateMember(data.NewGroupMember(-1, hostPort, 0, Leaving))

	//One Last Gossip to make sure someone knows I have left
	self.doUserTableGossip()
	fmt.Println("I am done here")
}

func (self *Ring) callForSuccessor(myKey int, address string) *data.GroupMember {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	//Get Successor
	argi := &myKey
	fmt.Printf("myKey : %d", myKey)
	var response *data.GroupMember
	err = client.Call("Ring.GetSuccessor", argi, &response)
	if response == nil {
		fmt.Println("No successor : only member in group")
	}
	self.Successor = response
	fmt.Println("Found Successor")
	self.updateMember(self.Successor)
	return response

}

// Gossip members from current table to a random member
func (self *Ring) doUserTableGossip() {
	if self.Active == false {
		return
	}
	tableLength := self.UserKeyTable.Len()

	// Nobody in the list yet
	if tableLength < 1 {
		return
	}
	receiver := self.getRandomMember()
	//fmt.Println(receiver.Address)
	for _, subject := range self.Usertable {

		if subject.Address != net.JoinHostPort(self.Address, self.Port) {
			subject.IncrementHeartBeat()
		}
		if subject.Heartbeat > heartbeatThreshold && subject.Id != -1 {
			log.Println("MACHINE DEAD!", subject.Id, subject.Heartbeat)
			//Deletes the member in the userkeytable
			self.updateMember(data.NewGroupMember(-1, subject.Address, subject.Heartbeat, Leaving))
		}
		if subject.Id != receiver.Id {
			self.doGossip(subject, receiver)
		}
	}
}

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
func (self *Ring) doGossip(subject, receiver *data.GroupMember) (err error) {
	// The message we are sending over UDP, subject can be nil
	//fmt.Println(subject.Id)
	msg := "GOSSIP|%|" + data.Marshal(subject)
	return self.sendMessageWithPort(msg, receiver.Address)
}

func (self *Ring) sendMessageWithPort(msg, address string) (err error) {
	msg = self.Port + "<PORT>" + msg
	return sendMessage(msg, address)
}

func sendMessage(message, address string) (err error) {
	var raddr *net.UDPAddr
	if raddr, err = net.ResolveUDPAddr("udp", address); err != nil {
		log.Panic(err)
	}

	var con *net.UDPConn
	con, err = net.DialUDP("udp", nil, raddr)
	//log.Printf("Sending '%s' to %s..", message, raddr)
	logger.Log("INFO", "Sending "+message)
	if _, err = con.Write([]byte(message)); err != nil {

		log.Panic("Writing to UDP:", err)
		logger.Log("ERROR", "Writing to UDP")
	}

	return
}

func (self *Ring) PrintMembers() {

	fmt.Println("Printiing Members")
	start := self.UserKeyTable.Min()
	for i := 0; i < self.UserKeyTable.Len(); i++ {
		fmt.Println(start.Item().(data.LocationStore))
		start = start.Next()
	}

}

func (self *Ring) PrintData() {

	fmt.Println("Printing Data")
	start := self.KeyValTable.Min()
	for i := 0; i < self.KeyValTable.Len(); i++ {
		fmt.Println(start.Item().(data.DataStore))
		start = start.Next()
	}
}

func createUDPListener(hostPort string) (conn *net.UDPConn, err error) {

	var udpaddr *net.UDPAddr
	if udpaddr, err = net.ResolveUDPAddr("udp", hostPort); err != nil {
		return
	}

	conn, err = net.ListenUDP("udp", udpaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	fmt.Println("UDP listener created")

	return
}
