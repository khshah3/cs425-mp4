package main

import (
	"./logger"
	"./ring"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

	go ring.Gossip()

	firstInGroup := groupMember == ""
	if !firstInGroup {
		ring.JoinGroup(groupMember)
		logger.Log("JOIN", "Gossiping new member to the group")
	} else {
		ring.FirstMember(hostPort)
	}

	//UDP
	go ring.ReceiveDatagrams(firstInGroup)

	if err != nil {
		fmt.Println("Ring addition failed:", err)
		logger.Log("FAILURE", "Ring could not be created")
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.SplitN(line, " ", 3)
		var key, val string
		var ikey int

		if len(words) > 1 {
			key = words[1]
			ikey, _ = strconv.Atoi(key)
			if len(words) > 2 {
				val = words[2]
			}
		}

		switch words[0] {
		case "insert":
			ring.Insert(ikey, val)
		case "update":
			ring.Update(ikey, val)
		case "remove":
			ring.Remove(ikey)
		case "lookup":
			start := time.Now()
			ring.Lookup(ikey)
			elapsed := time.Now().Sub(start)
			fmt.Println("ELAPSED TIME:", elapsed)
		case "leave":
			fmt.Println("Leaving Group")
			ring.LeaveGroup()
			goto Done
		case "show":
			ring.PrintMembers()
			ring.PrintData()
		}
	}
Done:
	if err := scanner.Err(); err != nil {
		log.Panic("Scanning stdin", err)
	}

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
