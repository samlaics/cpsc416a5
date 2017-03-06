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
	"net/url"
	"os"
	"strings"
)

var workerConns []string
var workerToDomainList map[string][]string // workerIP -> array of domains that it owns

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

// Request that client sends in RPC call to MServer.GetWorkers
type GetWorkersReq struct{}

// Response to MServer.GetWorkers
type GetWorkersRes struct {
	WorkerIPsList []string // List of workerIP string
}

/////////

// Request that client sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP
}

// Request that server sends in RPC call to MWorker.CrawlWebsite
type MCrawlWebsiteReq struct {
	URI                string // URI of the website to measure
	Depth              int    // Depth to crawl to from URL
	WorkerToDomainList map[string][]string
}

// Response to MWorker.CrawlWebsite
type MCrawlWebsiteRes struct{}

/////////

// Request that client sends in RPC call to MServer.Domains
type DomainsReq struct {
	WorkerIP string // IP of worker
}

// Response to MServer.Domains
type DomainsRes struct {
	Domains []string // List of domain string
}

/////////

// Request that client sends in RPC call to MServer.Overlap
type OverlapReq struct {
	URL1 string // URL arg to Overlap
	URL2 string // The other URL arg to Overlap
}

// Response to MServer.Overlap
type OverlapRes struct {
	NumPages int // Computed overlap between two URLs
}

/////////////// /RPC structs

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
			workerIPPort := conn.RemoteAddr().String()
			ip := strings.Split(workerIPPort, ":")[0]
			workerConns = append(workerConns, ip)
			fmt.Fprintf(os.Stderr, "added %s to ip list\n", ip)
			conn.Close()
		}
	}()

	// // udp server for ping pong-ing messages
	// go func() {
	// 	listener := getConnection(":8431")
	// 	wbuf := []byte("bye")
	// 	rbuf := make([]byte, 1024)
	// 	for {
	// 		_, remote_addr, err := listener.ReadFromUDP(rbuf)
	// 		checkError("", err, false)
	// 		go func() {
	// 			listener.WriteToUDP(wbuf, remote_addr)
	// 		}()
	// 	}
	// }()

	<-done
}

// MServer.Crawl
// Instructs the system to crawl the web starting at URL to a certain depth.
// For example, if depth is 0 then this should just crawl URL.
// If depth is 1, then this should crawl URL and then recursively crawl all pages linked from URL (with depth 0).
// This call must return the IP of the worker that is assigned as owner of the domain for URL.
func (m *MServer) Crawl(request CrawlReq, reply *CrawlRes) error {
	// TODO: check workerToDomainList to see if a worker already owns url domain
	// if so, ask that worker to crawl the url; otherwise do below
	stats := make(map[string]LatencyStats)
	uri := request.URL
	u, _ := url.Parse(uri)
	domain := "http://" + u.Host
	depth := request.Depth
	numReq := 5
	for _, ip := range workerConns {
		client, err := rpc.Dial("tcp", ip+":7369")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not connect to %s\n", ip+":7369")
		}
		var ls LatencyStats
		WebReq := MWebsiteReq{
			URI:              domain,
			SamplesPerWorker: numReq,
		}
		err = client.Call("MWorker.MeasureWebsite", WebReq, &ls)
		if err != nil {
			fmt.Fprintf(os.Stderr, "got nothing back from %s\n", ip+":7369")
		}
		stats[ip] = ls
	}
	var bestIP string
	minLatency := 99999
	for ip, ls := range stats {
		if ls.Min < minLatency {
			minLatency = ls.Min
			bestIP = ip
		}
	}
	// add worker ip -> domain to workerToDomainList
	workerToDomainList[bestIP] = append(workerToDomainList[bestIP], domain)
	// TODO: tell bestIP to crawl url
	// bestIP will crawl url, and if depth > 0, will compare crawled urls with known urls
	// if there are unknown urls, the bestIP worker will call findBestWorker to continue crawl
	// and will call RPC to update workerToDomainList on server
	client, err := rpc.Dial("tcp", bestIP+":7369")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not connect to %s\n", bestIP+":7369")
	}
	CrawlSiteReq := MCrawlWebsiteReq{
		URI:                uri,
		Depth:              depth,
		WorkerToDomainList: workerToDomainList,
	}
	var crawlRes MCrawlWebsiteRes
	err = client.Call("MWorker.CrawlWebsite", CrawlSiteReq, &crawlRes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "got nothing back from %s\n", bestIP+":7369")
	}

	*reply = CrawlRes{
		WorkerIP: bestIP,
	}
	return nil
}

// MServer.Overlap
// Returns the number of pages in the overlap of the worker domain page graphs rooted at URL1 and URL2
func (m *MServer) Overlap(request OverlapReq, reply *OverlapRes) error {
	*reply = OverlapRes{}
	return nil
}

// MServer.GetWorkers
// Returns the list of worker IPs connected to the server
func (m *MServer) GetWorkers(request GetWorkersReq, reply *GetWorkersRes) error {
	*reply = GetWorkersRes{
		WorkerIPsList: workerConns, // list of ip's
	}
	return nil
}

// MServer.Domains
// Returns a list of domains owned by the worker with IP workerIP
func (m *MServer) Domains(request DomainsReq, reply *DomainsRes) error {
	ip := request.WorkerIP
	domains := workerToDomainList[ip]
	*reply = DomainsRes{
		Domains: domains,
	}
	return nil
}

// functions from previous assignment solutions

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
