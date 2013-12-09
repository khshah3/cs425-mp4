package main

import (
	"./logger"
	"./ring"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// The operations

func main() {

	var (
		listenPort     string
		groupMember    string
		faultTolerance int
	)

	flag.StringVar(&listenPort, "l", "4567", "port to bind for UDP listener")
	flag.StringVar(&groupMember, "g", "", "address of an existing group member")
	flag.IntVar(&faultTolerance, "f", 0, "Use fault tolerance")
	flag.Parse()

	log.Println("Start server on port", listenPort)
	log.Println("Fault Tolernace", faultTolerance)

	hostPort := getHostPort(listenPort)

	//logger.Log("INFO", "Start Server on Port"+listenPort)

	//Add itself to the usertable - join
	ring, err := ring.NewMember(hostPort, faultTolerance)

	firstInGroup := groupMember == ""
	if !firstInGroup {
		ring.JoinGroup(groupMember)
		logger.Log("JOIN", "Gossiping new member to the group")
	} else {
		ring.FirstMember(hostPort)
		go ring.Gossip()
	}

	//UDP
	go ring.ReceiveDatagrams(firstInGroup)

	if err != nil {
		fmt.Println("Ring addition failed:", err)
		logger.Log("FAILURE", "Ring could not be created")
	}

	TESTCASES := 100000

	for i := 0; i < TESTCASES; i++ {
		k := rand.Intn(1000000)
		start := time.Now()
		ring.Lookup(k, 0)
		elapsed := time.Now().Sub(start)
		fmt.Println("ELAPSED\t", elapsed.Seconds())
	}
	ring.LeaveGroup()
}

func getHostPort(port string) (hostPort string) {

	name, err := os.Hostname()
	if err != nil {
		fmt.Printf("Oops: %v\n", err)
		return
	}
	addrs, err := net.LookupHost(name)
	if err != nil {
		fmt.Printf("Oops: %v\n", err)
		return
	}

	hostPort = net.JoinHostPort(addrs[0], port)
	return hostPort
}
