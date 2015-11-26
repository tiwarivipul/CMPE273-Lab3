package main

// Lab 3  To understand Consistent Hashing....

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

var kv1, kv2, kv3 []KeyValuePair
var int1, int2, int3 int

// struct to define Key, value pairs...
type KeyValuePair struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type kvPair []KeyValuePair

func (a kvPair) Len() int           { return len(a) }
func (a kvPair) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a kvPair) Less(i, j int) bool { return a[i].Key < a[j].Key }

func GetKeyValue(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {

	port := strings.Split(request.Host, ":")
	if port[1] == "3000" {
		sort.Sort(kvPair(kv1))
		result, _ := json.Marshal(kv1)
		fmt.Fprintln(rw, string(result))
	} else if port[1] == "3001" {
		sort.Sort(kvPair(kv2))
		result, _ := json.Marshal(kv2)
		fmt.Fprintln(rw, string(result))
	} else {
		sort.Sort(kvPair(kv3))
		result, _ := json.Marshal(kv3)
		fmt.Fprintln(rw, string(result))
	}
}

// Defining PUT functions for key value pairs........

func PutKeyValue(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {

	port := strings.Split(request.Host, ":")
	key, _ := strconv.Atoi(p.ByName("key_id"))
	if port[1] == "3000" {
		kv1 = append(kv1, KeyValuePair{key, p.ByName("value")})
		int1++
	} else if port[1] == "3001" {
		kv2 = append(kv2, KeyValuePair{key, p.ByName("value")})
		int2++
	} else {
		kv3 = append(kv3, KeyValuePair{key, p.ByName("value")})
		int3++
	}
}

// Defining GET functions for all key value pairs........

func GetAllKeyValue(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	out := kv1
	ind := int1
	port := strings.Split(request.Host, ":")
	if port[1] == "3001" {
		out = kv2
		ind = int2
	} else if port[1] == "3002" {
		out = kv3
		ind = int3
	}
	key, _ := strconv.Atoi(p.ByName("key_id"))
	for i := 0; i < ind; i++ {
		if out[i].Key == key {
			result, _ := json.Marshal(out[i])
			fmt.Fprintln(rw, string(result))
		}
	}
}

func main() {
	int1 = 0
	int2 = 0
	int3 = 0

	mux := httprouter.New()

	// Performing GET, PUT functions....

	mux.GET("/keys", GetKeyValue)
	mux.GET("/keys/:key_id", GetAllKeyValue)
	mux.PUT("/keys/:key_id/:value", PutKeyValue)

	// Listening on Different ports....

	go http.ListenAndServe(":3000", mux)
	go http.ListenAndServe(":3001", mux)
	go http.ListenAndServe(":3002", mux)
	select {}
}
