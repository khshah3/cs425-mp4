package data

import (
	"../logger"
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

/*
  This will be responsible for the data conversion. It allows us to take an object like
  GroupMember and change it to an array of bytes to send over UDP. Then, on the other end,
  it should convert these bytes back into the original object.
*/

const (
	delim = "$$$"
)

// Serialize a GroupMember for transmission over UDP

func Marshal(member *GroupMember) (serialized string) {
	if member == nil {
		return "NIL"
	}

	buf := new(bytes.Buffer)
	serialized = fmt.Sprintf("%d%s%s%s%d%s%d", member.Id, delim, member.Address, delim, member.Heartbeat, delim, member.Movement)
	byteSerialized := []byte(serialized)
	err := binary.Write(buf, binary.LittleEndian, byteSerialized)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	logger.Log("MARSHAL", fmt.Sprintf("<%s, %d> ---> %s", member.Id, member.Heartbeat, serialized))
	//log.Printf("<%s, %d> ---> %s", member.Id, member.Heartbeat, serialized)
	return string(buf.Bytes())
}

// Deserialize a transmitted GroupMember
func Unmarshal(serialized string) (member *GroupMember) {

	if serialized == "NIL" {
		return nil
	}

	byteSerialized := []byte(serialized)
	buf := bytes.NewBuffer(byteSerialized)
	err := binary.Read(buf, binary.LittleEndian, &byteSerialized)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	serialized = string(byteSerialized)
	fields := strings.SplitN(serialized, delim, 4)
	id, address, hbs, mve := fields[0], fields[1], fields[2], fields[3]
	hb, _ := strconv.Atoi(hbs)
	ids, _ := strconv.Atoi(id)
	mvs, _ := strconv.Atoi(mve)
	return NewGroupMember(ids, address, hb, mvs)

}
