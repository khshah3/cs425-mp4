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
		client         int
	)

	flag.StringVar(&listenPort, "l", "4567", "port to bind for UDP listener")
	flag.IntVar(&client, "c", 0, "Use 1 if you are a client")
	flag.StringVar(&groupMember, "g", "", "address of an existing group member")
	flag.IntVar(&faultTolerance, "f", 0, "Use fault tolerance")
	flag.Parse()

	log.Println("Start server on port", listenPort)
	log.Println("Fault Tolernace", faultTolerance)

	hostPort := getHostPort(listenPort)
	fmt.Println(client)
	//logger.Log("INFO", "Start Server on Port"+listenPort)

	//Add itself to the usertable - join
	ring, err := ring.NewMember(hostPort, faultTolerance)

	firstInGroup := groupMember == ""
	if !firstInGroup {
		if client == 1 {
			fmt.Println("YEs")
			ring.ClientMember(hostPort)
			ring.FirstMember(groupMember)
		} else {

			ring.JoinGroup(groupMember)
			logger.Log("JOIN", "Gossiping new member to the group")

		}
	} else {
		if client == 1 {
			fmt.Println("There are no servers for you")
			return
		}
		ring.FirstMember(hostPort)
	}
	go ring.Gossip()

	//UDP
	go ring.ReceiveDatagrams(firstInGroup)

	if err != nil {
		fmt.Println("Ring addition failed:", err)
		logger.Log("FAILURE", "Ring could not be created")
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.SplitN(line, " ", 4)
		var val string
		var ikey int
		var consistency int
		consistency = -1

		if len(words) > 0 {
			consistency, _ = strconv.Atoi(words[0])

			if len(words) > 2 {
				ikey, _ = strconv.Atoi(words[2])

				if len(words) > 3 {
					val = words[3]
				}
			}

		}


		switch words[1] {
		case "insert":
			ring.Insert(ikey, val, consistency)
		case "update":
			ring.Update(ikey, val, consistency)
		case "remove":
			ring.Remove(ikey, consistency)
		case "lookup":
			start := time.Now()
			ring.Lookup(ikey, consistency)
			elapsed := time.Now().Sub(start)
			fmt.Println("ELAPSED TIME:", elapsed)
		case "leave":
			fmt.Println("Leaving Group")
			ring.LeaveGroup()
			goto Done
		case "show":
			ring.PrintMembers()
			ring.PrintData()
      ring.CmdLog.Print()
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
