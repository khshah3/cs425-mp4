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
	"net"
	"net/rpc"
	"strings"
	"time"
)

const (
	DataSentAndLeft = iota
	Leaving
	Stable
	Joining
)

const (
	heartbeatThreshold = 50
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
func (self *Ring) callSuccessorRPC(key int, function string, args *data.DataStore, consistency int) (result RpcResult) {
	client := self.dialSuccessor(key)
	defer client.Close()
	var err error
	if consistency == -1 {
		err = client.Call(function, args, &result)
	} else {

		function = function + "Consistent"
		consistentStore := data.NewConsistentDataStore(args, consistency)
		err = client.Call(function, consistentStore, &result)
	}

	if err != nil {
		fmt.Println("Error sending data:", err)
		result.Success = -2
		return
	}
	if result.Success != 1 {
		fmt.Println("Error storing data")
	}
	fmt.Printf("Data Size: %d \n", self.KeyValTable.Len())
	return result
}

/* The actual Operations exposed over RPC */
func (self *Ring) Insert(key int, val string, consistency int) {

	args := data.NewDataStore(key, val)
	result := self.callSuccessorRPC(key, "Ring.SendData", args, consistency)

	if result.Member != nil && result.Success != 1 {
		self.updateMember(result.Member)
		self.Insert(key, val, consistency)
	} else {
		timeout := 3
		i := 0
		//Found New Member
		fmt.Println(result.Success)
		for result.Success == -2 && i < timeout {
			i++
			result = self.callSuccessorRPC(key, "Ring.SendData", args, consistency)

		}
	}

}

func (self *Ring) Update(key int, val string, consistency int) {
	args := data.NewDataStore(key, val)
	result := self.callSuccessorRPC(key, "Ring.UpdateData", args, consistency)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Update(key, val, consistency)
	}
}

func (self *Ring) Remove(key int, consistency int) {
	args := data.NewDataStore(key, "")
	result := self.callSuccessorRPC(key, "Ring.RemoveData", args, consistency)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Remove(key, consistency)
	}
}

func (self *Ring) Lookup(key int, consistency int) {
	args := data.NewDataStore(key, "")
	result := self.callSuccessorRPC(key, "Ring.GetData", args, consistency)
	if result.Success != 1 && result.Member != nil {
		self.updateMember(result.Member)
		self.Lookup(key, consistency)
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

	lastKey := -1
	if member != nil {
		lastKey = member.Id
	}

	// Add new member if one doesn't already exist
	if member == nil {
		self.Usertable[updatedMember.Address] = updatedMember
		//We dont want to add to the server location table
		if key == -204 {
			return
		}
		if self.UserKeyTable.Get(data.LocationStore{key, ""}) == nil {
			self.UserKeyTable.Insert(data.LocationStore{key, updatedMember.Address})
		} else {
			fmt.Println("ERROR: Two members with same key")
		}
		return
	}

	// Update the existing member
	if member.Heartbeat > updatedMember.Heartbeat {
		member.SetHeartBeat(0)
	}

	//Client case - We dont want to do anything
	if key == -204 {
		return
	}

	// TODO Not sure what's going on here?
	if member.Movement < movement {
		fmt.Println("You should not be able to join if you already exist or stay if you already started leaving")
		return
	}

	self.Usertable[updatedMember.Address] = updatedMember
	if ((movement == Joining || member.Movement == Joining) && (key > lastKey)) ||
		((movement == Leaving || member.Movement == Leaving) && (key < lastKey)) ||
		((movement == DataSentAndLeft || member.Movement == DataSentAndLeft) && (key < lastKey)) {

		fmt.Printf("Deleting member with ID %d FROM %s", lastKey, updatedMember.Address)
		self.UserKeyTable.DeleteWithKey(data.LocationStore{lastKey, ""})

		if key != -1 {
			fmt.Printf("Inserting member with ID %d FROM %s", key, updatedMember.Address)
			self.UserKeyTable.Insert(data.LocationStore{key, updatedMember.Address})
		}
	}
}

func (self *Ring) FirstMember(portAddress string) {
	key := data.Hasher(portAddress)
	fmt.Println("Found")
	fmt.Println(key)
	newMember := data.NewGroupMember(key, portAddress, 0, Stable)
	self.updateMember(newMember)
}

func (self *Ring) ClientMember(portAddress string) {
	key := -204
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
	userTableInterval := 500 * time.Millisecond

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

	fmt.Println(senderAddr)
	sender := self.Usertable[senderAddr]
	if sender != nil {
		//fmt.Println("Updating")
		self.Usertable[senderAddr].SetHeartBeat(0)
	} else {

	}
	self.updateMember(subjectMember)
	//fmt.Println("Updating Heartbeat to ", senderAddr, self.Usertable[senderAddr].Heartbeat)
}

/*func (self *Ring) ClientJoin(address string) {

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	hostPort := net.JoinHostPort(self.Address, self.Port)

	//Client key value
	//
	//
	key := -201

	client, err = rpc.DialHTTP("tcp", successor.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println(successor)
	//Get smallest key less then key and initiate data transfer
	var data_t []*data.DataStore
	err = client.Call("Ring.AddClient", argi, &data_t)

}*/

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
	var data_t []*data.DataStore
	err = client.Call("Ring.GetEntryData", argi, &data_t)

	//TODO:: Iterate throught array and add items like below except all at once as shown.  Straightforward.

	length := len(data_t)

	for i := 0; i < length; i++ {

		//Insert Key into my table
		self.KeyValTable.Insert(*(data_t[i]))

		//Insert Value of Key as my Id
		newMember := data.NewGroupMember(data_t[i].Key, hostPort, 0, Joining)
		self.updateMember(newMember)

		//Start Gossiping
		if self.isGossiping == false {
			go self.Gossip()
		}
	}

	if self.isGossiping == false {
		go self.Gossip()
		fmt.Println("Am i done")
	}
	//Make hashed key my id
	finalMember := data.NewGroupMember(hashedKey, hostPort, 0, Stable)
	self.updateMember(finalMember)
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
	self.bulkDataDeleteAndSend("Ring.SendLeaveData", successor)

	self.updateMember(data.NewGroupMember(-1, hostPort, 0, DataSentAndLeft))

	//One Last Gossip to make sure someone knows I have left
	self.doUserTableGossip()
	fmt.Println("I am done here")
}

//Send all your data deleting them as you go to a receving member
func (self *Ring) bulkDataDeleteAndSend(function string, receiver *data.GroupMember) {

	hostPort := net.JoinHostPort(self.Address, self.Port)
	key := self.Usertable[hostPort].Id

	client, err := rpc.DialHTTP("tcp", receiver.Address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	fmt.Println(self.KeyValTable.Len())
	NextLessThen := self.KeyValTable.FindLE(data.DataStore{key, ""})
	for NextLessThen != self.KeyValTable.NegativeLimit() {

		sendingData := NextLessThen.Item().(data.DataStore)
		fmt.Println(sendingData)
		sendDataPtr := &sendingData
		var result RpcResult
		err = client.Call(function, sendDataPtr, &result)
		if err != nil {
			fmt.Println("Error sending data", err)
			return
		}
		fmt.Println(result)
		if result.Success == 1 {
			fmt.Println("Data Succesfully sent")
			self.KeyValTable.DeleteWithIterator(NextLessThen)
			self.updateMember(data.NewGroupMember(sendingData.Key, hostPort, 0, Leaving))
		} else {
			fmt.Println("Error sending data", err)
			break
		}
		NextLessThen = self.KeyValTable.FindLE(data.DataStore{sendingData.Key, ""})
	}
}

//Send all your data to replicas
func (self *Ring) bulkDataSendToReplicas() {

	min := self.KeyValTable.Min()
	key := self.getKey()

	//Tree is empty
	if min.Equal(self.KeyValTable.Limit()) {
		return
	}
	for min != self.KeyValTable.Limit() {
		item := min.Item().(data.DataStore)
		self.writeToReplicas(&item, key)
		fmt.Println(min.Item().(data.DataStore))
		min = min.Next()
	}

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

	//Get predecessor
	predecessorKey := self.getPredecessorKey(self.getKey())
	receiver := self.getRandomMember()
	//fmt.Println(receiver.Id)
	//fmt.Println(receiver.Address)
	for _, subject := range self.Usertable {

		if subject.Address != net.JoinHostPort(self.Address, self.Port) {
			subject.IncrementHeartBeat()
		}
		//fmt.Println(subject.Id, subject.Heartbeat)
		if subject.Heartbeat > heartbeatThreshold && subject.Id != -1 {
			log.Println("MACHINE DEAD!", subject.Id, subject.Heartbeat)
			if subject.Id == predecessorKey {

				//TODO:: Update replicase
				fmt.Println("Now you need to update your replicas")
				self.bulkDataSendToReplicas()

			}
			//Deletes the member in the userkeytable
			self.updateMember(data.NewGroupMember(-1, subject.Address, subject.Heartbeat, Leaving))
		}
		if subject.Id != receiver.Id {
			self.doGossip(subject, receiver)
		}
	}
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
