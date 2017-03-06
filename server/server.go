/*
Server for assignment 4 for UBC CS 416 2016 W2.

Usage:
$ go run server.go [worker-incoming ip:port] [client-incoming ip:port]
    [worker-incoming ip:port] : the IP:port address that workers use to connect to the server
    [client-incoming ip:port] : the IP:port address that clients use to connect to the server

Example:
$ go run server.go 127.0.0.1:2020 127.0.0.1:7070

*/

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
)

var workerConns []string

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

type LatencyStatsv2 struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
	Hash   [16]byte
}

/////////////// RPC structs

// Resource server type.
type MServer int

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

// Response to:
// MServer.MeasureWebsite:
//   - latency stats per worker to a *URI*
//   - (optional) Diff map
// MServer.GetWorkers
//   - latency stats per worker to the *server*
type MRes struct {
	Stats map[string]LatencyStats    // map: workerIP -> LatencyStats
	Diff  map[string]map[string]bool // map: [workerIP x workerIP] -> True/False
}

// Request that client sends in RPC call to MServer.GetWorkers
type MWorkersReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

// Main workhorse method.
func main() {
	args := os.Args[1:]

	// Missing command line args.
	if len(args) != 2 {
		fmt.Println("Usage: server.go [worker-incoming ip:port] [client-incoming ip:port]")
		return
	}

	// Extract the command line args.
	listen_worker_ip_port := args[0]
	listen_client_ip_port := args[1]

	done := make(chan int)

	// Set up RPC for client so it can talk with server
	go func() {
		cServer := rpc.NewServer()
		c := new(MServer)
		cServer.Register(c)

		lc, err := net.Listen("tcp", listen_client_ip_port)
		checkError("", err, true)
		for {
			conn, err := lc.Accept()
			checkError("", err, false)
			go cServer.ServeConn(conn)
		}
	}()

	// Set up server to listen for worker connections and record their ip's
	go func() {
		lw, err := net.Listen("tcp", listen_worker_ip_port)
		checkError("", err, true)
		for {
			conn, err := lw.Accept()
			checkError("", err, false)
			workerIPPort := conn.RemoteAddr()
			workerConns = append(workerConns, workerIPPort.String())
			fmt.Fprintf(os.Stderr, "added %s to ip list\n", workerIPPort)
			conn.Close()
		}
	}()

	// udp server for ping pong-ing messages
	go func() {
		listener := getConnection(":8431")
		wbuf := []byte("bye")
		rbuf := make([]byte, 1024)
		for {
			_, remote_addr, err := listener.ReadFromUDP(rbuf)
			checkError("", err, false)
			go func() {
				listener.WriteToUDP(wbuf, remote_addr)
			}()
		}
	}()

	<-done
}

func (m *MServer) MeasureWebsite(request MWebsiteReq, reply *MRes) error {
	statsv2 := make(map[string]LatencyStatsv2)
	uri := request.URI
	numReq := request.SamplesPerWorker
	fmt.Fprintf(os.Stderr, "client requested %d samples per worker to %s\n", numReq, uri)
	// send RPC calls for workers to fetch webpage
	for _, k := range workerConns {
		ip := strings.Split(k, ":")[0]
		client, err := rpc.Dial("tcp", ip+":7369")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not connect to %s\n", ip+":7369")
		}
		var ls LatencyStatsv2
		err = client.Call("MWorker.MeasureWebsite", request, &ls)
		if err != nil {
			fmt.Fprintf(os.Stderr, "got nothing back from %s\n", ip+":7369")
		}
		statsv2[ip] = ls
	}
	stats := make(map[string]LatencyStats)
	diff := make(map[string]map[string]bool)
	for k, v := range statsv2 {
		var ls LatencyStats
		ls.Min = v.Min
		ls.Median = v.Median
		ls.Max = v.Max
		stats[k] = ls
		diff[k] = make(map[string]bool)
		for k2, v2 := range statsv2 {
			if v.Hash == v2.Hash {
				diff[k][k2] = false
			} else {
				diff[k][k2] = true
			}
		}
	}
	*reply = MRes{
		Stats: stats, // map: workerIP -> LatencyStats
		Diff:  diff,  // map: [workerIP x workerIP] -> True/False
	}
	return nil
}

// MServer.GetWorkers
// latency stats per worker to the *server*
func (m *MServer) GetWorkers(request MWorkersReq, reply *MRes) error {
	stats := make(map[string]LatencyStats)
	numReq := request.SamplesPerWorker
	fmt.Fprintf(os.Stderr, "client requested %d samples per worker to server\n", numReq)
	// send RPC calls to send udp message to server
	for _, k := range workerConns {
		ip := strings.Split(k, ":")[0]
		client, err := rpc.Dial("tcp", ip+":7369")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not connect to %s\n", ip+":7369")
		}
		var ls LatencyStats
		err = client.Call("MWorker.GetWorkers", request, &ls)
		if err != nil {
			fmt.Fprintf(os.Stderr, "got nothing back from %s\n", ip+":7369")
		}
		stats[ip] = ls
	}
	*reply = MRes{
		Stats: stats, // map: workerIP -> LatencyStats
	}
	return nil
}

// functions from previous assignments

func getConnection(ip string) *net.UDPConn {
	lAddr, err := net.ResolveUDPAddr("udp", ip)
	checkError("", err, false)
	l, err := net.ListenUDP("udp", lAddr)
	checkError("", err, false)
	return l
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
