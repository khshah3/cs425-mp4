package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
  "bufio"
  "strings"
  "./data"
  "./ring"
  "./logger"
)

func main() {
  var (
    dataFile string
		listenPort     string
		groupMember    string
  )

	flag.StringVar(&listenPort, "l", "4567", "port to bind for UDP listener")
	flag.StringVar(&groupMember, "g", "", "address of an existing group member")
  flag.StringVar(&dataFile, "d", "", "The ##-delimited file to initialize the dictionary definitions from")
  flag.Parse()


  // Copy-pasted from myks.go
  //
	hostPort := getHostPort(listenPort)

	//Add itself to the usertable - join
	ring, err := ring.NewMember(hostPort, 0)

  ring.ClientMember(hostPort)
  ring.FirstMember(groupMember)

	go ring.Gossip()

	//UDP
	go ring.ReceiveDatagrams(false)

	if err != nil {
		fmt.Println("Ring addition failed:", err)
		logger.Log("FAILURE", "Ring could not be created")
	}


  // Initialize the cluster with the data
  if dataFile != "" {
    log.Printf("Loading data from %s", dataFile)
    loadDataFile(dataFile, ring)
  }

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		query := strings.TrimSpace(scanner.Text())
    key := data.Hasher(query)
    ring.Lookup(key, 0)

		if query == "leave" {
			fmt.Println("Leaving Group")
			ring.LeaveGroup()
			goto Done
		}
	}
Done:
	if err := scanner.Err(); err != nil {
		log.Panic("Scanning stdin", err)
	}


}


func loadDataFile(path string, serverRing *ring.Ring) error {
  file, err := os.Open(path)
  if err != nil {
    return err
  }
  defer file.Close()

  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
		kv := strings.SplitN(scanner.Text(), "##", 2)
    word, def := kv[0], kv[1]
    key := data.Hasher(word)

    log.Printf("Inserting %s (%s)", word, key)
    serverRing.Insert(key, def, 0)
  }
  return scanner.Err()
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
