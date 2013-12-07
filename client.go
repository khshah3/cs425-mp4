package client

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

func main() {

	var (
		groupMember string
	)

	flag.StringVar(&groupMember, "g", "", "address of an existing group member")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.Split(line, " ")
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
		}
	}
Done:
	if err := scanner.Err(); err != nil {
		log.Panic("Scanning stdin", err)
	}

}
