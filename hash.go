package main

import "strings"

type Hash struct {
	Number  int
	Payload string
}

type HashArray []Hash

func (hashArray HashArray) Len() int {
	return len(hashArray)
}

func (hashArray HashArray) Less(i, j int) bool {
	return hashArray[i].Number < hashArray[j].Number
}

func (hashArray HashArray) Swap(i, j int) {
	el := hashArray[j]
	hashArray[j] = hashArray[i]
	hashArray[i] = el
}

func (hashArray HashArray) Concat() string {
	var hashes []string

	for ind := range hashArray {
		hashes = append(hashes, hashArray[ind].Payload)
	}

	return strings.Join(hashes, "")
}
