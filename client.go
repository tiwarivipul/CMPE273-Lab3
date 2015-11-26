package main

// Lab 3  To understand Consistent Hashing....

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
)

type HCircle []uint32

// struct to define Key, value pairs...

type KeyValuePair struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (hc1 HCircle) Len() int {
	return len(hc1)
}

func (hc1 HCircle) Less(i, j int) bool {
	return hc1[i] < hc1[j]
}

func (hc1 HCircle) Swap(i, j int) {
	hc1[i], hc1[j] = hc1[j], hc1[i]
}

type Node struct {
	Id int
	IP string
}

func UpdatedNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type ConsistentHash struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	Circle    HCircle
}

// Defining function for New consistent Hasing ........

func NeuConsistHashing() *ConsistentHash {
	return &ConsistentHash{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		Circle:    HCircle{},
	}
}

func (hc1 *ConsistentHash) MergeNode(node *Node) bool {

	if _, ok := hc1.IsPresent[node.Id]; ok {
		return false
	}
	str := hc1.ReturnNodeIP(node)
	hc1.Nodes[hc1.GettingHValue(str)] = *(node)
	hc1.IsPresent[node.Id] = true
	hc1.SortingHCircle()
	return true
}

func (hc1 *ConsistentHash) SortingHCircle() {
	hc1.Circle = HCircle{}
	for k := range hc1.Nodes {
		hc1.Circle = append(hc1.Circle, k)
	}
	sort.Sort(hc1.Circle)
}

func (hc1 *ConsistentHash) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hc1 *ConsistentHash) GettingHValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hc1 *ConsistentHash) Get(key string) Node {
	hash := hc1.GettingHValue(key)
	i := hc1.DetectNode(hash)
	return hc1.Nodes[hc1.Circle[i]]
}

// Function foe detecting new node......

func (hc1 *ConsistentHash) DetectNode(hash uint32) int {
	i := sort.Search(len(hc1.Circle), func(i int) bool { return hc1.Circle[i] >= hash })
	if i < len(hc1.Circle) {
		if i == len(hc1.Circle)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hc1.Circle) - 1
	}
}

// Defining the function for PUT  in key value pairs..........

func PutKeyValue(circle *ConsistentHash, str string, input string) {
	ipAddress := circle.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Println(address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println("PUT Request successfully completed")
	}
}

// Defining the function for GET  to get all Key Values..........

func GetKeyValue(key string, circle *ConsistentHash) {
	var out KeyValuePair
	ipAddress := circle.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

// Defining the function for GET to get all the values..........

func GetAllKeyValue(address string) {

	var out []KeyValuePair
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func main() {

	circle := NeuConsistHashing()

	//  Different Ports.....

	circle.MergeNode(UpdatedNode(0, "127.0.0.1:3000"))
	circle.MergeNode(UpdatedNode(1, "127.0.0.1:3001"))
	circle.MergeNode(UpdatedNode(2, "127.0.0.1:3002"))

	if os.Args[1] == "PUT" {
		key := strings.Split(os.Args[2], "/")
		PutKeyValue(circle, key[0], key[1])
	} else if (os.Args[1] == "GET") && len(os.Args) == 3 {
		GetKeyValue(os.Args[2], circle)
	} else {
		GetAllKeyValue("http://127.0.0.1:3000/keys")
		GetAllKeyValue("http://127.0.0.1:3001/keys")
		GetAllKeyValue("http://127.0.0.1:3002/keys")
	}
}
