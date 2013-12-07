package data

import (
	"math"
)

//http://research.cs.vt.edu/AVresearch/hashing/strings.php
//Takes a string and returns a number between 0 - 1 millions
func Hasher(s string) (value int) {

	intLength := len(s) / 4
	sum := 0
	var mult int64
	var c string
	for j := 0; j < intLength; j++ {
		c = s[j*4 : (j*4)+4]
		mult = 1
		for k := 0; k < len(c); k++ {
			sum += int(c[k]) * int(mult)
			mult *= 256
		}
	}

  if len(s) % 4 != 0 {
    c = s[intLength*4 : ]
    mult = 1

    for k := 0; k < len(c); k++ {
      sum += int(c[k]) * int(mult)
      mult *= 256
    }
  }

	return (int)(math.Abs(float64(sum % 1000000)))
}
