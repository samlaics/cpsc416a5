/*
Worker for assignment 4 for UBC CS 416 2016 W2.

Usage:
$ go run worker.go [server ip:port]
	[server ip:port] : the address and port of the server (its worker-incoming ip:port).

*/

package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var server_ip_port string

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
type MWorker int

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

// Request that client sends in RPC call to MServer.GetWorkers
type MWorkersReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

// Main workhorse method.
func main() {
	args := os.Args[1:]

	// Missing command line args.
	if len(args) != 1 {
		fmt.Println("Usage: worker.go [server ip:port]")
		return
	}

	// Extract the command line args.
	server_ip_port = args[0]

	done := make(chan int)

	// Set up RPC so server can talk to it
	go func() {
		wServer := rpc.NewServer()
		w := new(MWorker)
		wServer.Register(w)

		lc, err := net.Listen("tcp", ":7369")
		checkError("", err, true)
		for {
			conn, err := lc.Accept()
			checkError("", err, false)
			go wServer.ServeConn(conn)
		}
	}()

	client, err := net.Dial("tcp", server_ip_port)
	checkError("", err, true)
	time.Sleep(3 * time.Second)
	client.Close()

	<-done
}

func (m *MWorker) MeasureWebsite(request MWebsiteReq, reply *LatencyStatsv2) error {
	var stats []int
	uri := request.URI
	numReq := request.SamplesPerWorker
	fmt.Fprintf(os.Stderr, "server requested %d samples to %s\n", numReq, uri)
	// fetch webpage
	var wg sync.WaitGroup
	wg.Add(numReq)
	var bodyHash [16]byte
	for i := 0; i < numReq; i++ {
		go func() {
			defer wg.Done()
			start := time.Now()
			response, err := http.Get(uri)
			checkError("", err, false)
			if err == nil {
				elapsed := time.Since(start)
				timeTaken := int(elapsed / time.Millisecond)
				fmt.Fprintf(os.Stderr, "rtt to %s was %d\n", uri, timeTaken)
				stats = append(stats, timeTaken)
				tmp, _ := ioutil.ReadAll(response.Body)
				hash := md5.New()
				hash.Write(tmp)
				bodyHash = md5.Sum(tmp)
				response.Body.Close()
			}
		}()
	}
	wg.Wait()
	fmt.Fprintf(os.Stderr, "site hash was %x\n", bodyHash)
	sort.Ints(stats)
	fmt.Fprintf(os.Stderr, "total samples actually taken was %d\n", len(stats))
	var min, max, median int
	if len(stats) == 0 {
		min = 0
		max = 0
		median = 0
	} else {
		min = stats[0]
		max = stats[len(stats)-1]
		median = stats[len(stats)/2]
	}
	*reply = LatencyStatsv2{
		Min:    min,    // min measured latency in milliseconds to host
		Median: median, // median measured latency in milliseconds to host
		Max:    max,    // max measured latency in milliseconds to host
		Hash:   bodyHash,
	}
	return nil
}

// MServer.GetWorkers
// latency stats per worker to the *server*
func (m *MWorker) GetWorkers(request MWorkersReq, reply *LatencyStats) error {
	var stats []int
	numReq := request.SamplesPerWorker
	fmt.Fprintf(os.Stderr, "client requested %d samples per worker to server\n", numReq)
	conn := getConnection(":8431")
	defer conn.Close()
	serverIP := strings.Split(server_ip_port, ":")[0]
	server := getAddr(serverIP + ":8431")
	wbuf := []byte("hi")
	rbuf := make([]byte, 1024)
	// send RPC calls to send udp message to server
	for i := 0; i < numReq; i++ {
		fmt.Fprintf(os.Stderr, "sending message to server\n")
		start := time.Now()
		conn.WriteToUDP(wbuf, server)
		conn.SetDeadline(time.Now().Add(time.Second))
		_, err := conn.Read(rbuf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "got nothing back\n")
			continue
		}
		elapsed := time.Since(start)
		timeTaken := int(elapsed / time.Millisecond)
		fmt.Fprintf(os.Stderr, "rtt to server was %d\n", timeTaken)
		stats = append(stats, timeTaken)
	}
	sort.Ints(stats)
	fmt.Fprintf(os.Stderr, "total samples actually taken was %d\n", len(stats))
	min := stats[0]
	max := stats[len(stats)-1]
	median := stats[len(stats)/2]
	*reply = LatencyStats{
		Min:    min,    // min measured latency in milliseconds to host
		Median: median, // median measured latency in milliseconds to host
		Max:    max,    // max measured latency in milliseconds to host
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

func getAddr(ip string) *net.UDPAddr {
	addr, err := net.ResolveUDPAddr("udp", ip)
	checkError("", err, false)
	return addr
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
