/*
Worker for assignment 4 for UBC CS 416 2016 W2.

Usage:
$ go run worker.go [server ip:port]
	[server ip:port] : the address and port of the server (its worker-incoming ip:port).

*/

package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

var server_ip_port string
var workerDomains []string             // domains this worker is responsible for
var workerGraph map[string][]GraphLink // url1 -> {url2,workerIPHostingUrl2}

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

// A link for workerGraph
type GraphLink struct {
	URI      string
	WorkerIP string
}

/////////////// RPC structs

// Resource server type.
type MWorker int

// Request that server sends in RPC call to MWorker.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

// Request that server sends in RPC call to MWorker.CrawlWebsite
type MCrawlWebsiteReq struct {
	URI       string // URI of the website to measure
	Depth     int    // Depth to crawl to from URL
	ServerRPC string // for the worker to make RPC calls
	WorkerIP  string // IP of the worker according to the server
}

// Response to MWorker.CrawlWebsite
type MCrawlWebsiteRes struct {
	MyIP string
}

// Request that worker sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP that owns the uri that was crawled
}

// Request that server sends in RPC call to MWorker.MeasureOverlap
type OverlapReqWorker struct {
	URL1      string // URL arg to Overlap
	URL2      string // The other URL arg to Overlap
	WorkerIP1 string
	WorkerIP2 string
}

// Response to MWorker.MeasureOverlap (respond to server who called it)
type OverlapRes struct {
	NumPages int // Computed overlap between two URLs
}

// Request that worker sends in RPC call to MWorker.WorkerOverlap
type OverlapWithWorkerReq struct {
	Subgraph []string
	URL      string // where to start subgraph on other worker
}

// Response that worker sends in RPC call to MWorker.WorkerOverlap
type OverlapWithWorkerRes struct {
	Subgraph []string
	NumPages int // num of overlap
}

/////////

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
	workerGraph = make(map[string][]GraphLink)

	// Set up RPC so server and other workers can talk to it
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

	// say hi to the server
	client, err := net.Dial("tcp", server_ip_port)
	checkError("", err, true)
	time.Sleep(3 * time.Second)
	client.Close()

	<-done
}

// MWorker.MeasureWebsite
// measure the latency to the website
func (m *MWorker) MeasureWebsite(request MWebsiteReq, reply *LatencyStats) error {
	var stats []int
	uri := request.URI
	numReq := request.SamplesPerWorker
	fmt.Fprintf(os.Stderr, "server requested %d samples to %s\n", numReq, uri)
	// fetch webpage
	var wg sync.WaitGroup
	wg.Add(numReq)
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
				response.Body.Close()
			}
		}()
	}
	wg.Wait()
	sort.Ints(stats)
	fmt.Fprintf(os.Stderr, "total samples actually taken was %d\n", len(stats))
	var min, max, median int
	if len(stats) == 0 {
		min = 999
		max = 0
		median = 0
	} else {
		min = stats[0]
		max = stats[len(stats)-1]
		median = stats[len(stats)/2]
	}
	*reply = LatencyStats{
		Min:    min,    // min measured latency in milliseconds to host
		Median: median, // median measured latency in milliseconds to host
		Max:    max,    // max measured latency in milliseconds to host
	}
	return nil
}

// MWorker.CrawlWebsite
// recursively crawl the website given until depth==0
func (m *MWorker) CrawlWebsite(request MCrawlWebsiteReq, reply *MCrawlWebsiteRes) error {
	uri := request.URI
	depth := request.Depth
	serverIP := strings.Split(server_ip_port, ":")[0]
	serverIPPort := serverIP + ":" + strings.Split(request.ServerRPC, ":")[1]
	myPubIP := request.WorkerIP
	base, err := url.Parse(uri)
	checkError("", err, false)

	// for all depths (including 0), add the uri that this rpc was called with
	// to the graph of this worker
	emptyGL := GraphLink{WorkerIP: "end"}
	if len(workerGraph[uri]) == 0 {
		workerGraph[uri] = append(workerGraph[uri], emptyGL)
	}
	if !contains(workerDomains, base.Host) {
		workerDomains = append(workerDomains, base.Host)
	}

	// if depth > 0,
	// call crawler code on this uri
	if depth > 0 {
		links := crawl(uri)
		// sanitize the links (clean up relative links)
		for i, link := range links {
			cleanLink := link
			fmt.Println("original link is:" + cleanLink)
			u, err := url.Parse(link)
			checkError("", err, false)
			cleanLink = base.ResolveReference(u).String()
			fmt.Println("sanitized link is:" + cleanLink)
			links[i] = cleanLink
		}
		// for all scraped links, continue scraping with depth-1 and/or instruct new workers to scrape
		// if there are new domains, send RPC to see where domain should live
		server, err := rpc.Dial("tcp", serverIPPort)
		checkError("", err, false)
		myself, err := rpc.Dial("tcp", myPubIP+":7369")
		checkError("", err, false)
		for _, link := range links {
			u, err := url.Parse(link)
			checkError("", err, false)
			domain := u.Host
			if contains(workerDomains, domain) {
				// crawled link definitely belongs on this worker
				// call own RPC to scrape this link
				crawlReq := MCrawlWebsiteReq{
					URI:       link,
					Depth:     depth - 1,
					ServerRPC: serverIPPort,
					WorkerIP:  myPubIP,
				}
				var crawlRes MCrawlWebsiteRes
				err = myself.Call("MWorker.CrawlWebsite", crawlReq, &crawlRes)
				gl := GraphLink{
					URI:      link,
					WorkerIP: myPubIP,
				}
				if !containsLink(workerGraph[uri], gl) {
					workerGraph[uri] = append(workerGraph[uri], gl)
					fmt.Fprintf(os.Stderr, "\nadding gl from %s to %#v\n", uri, gl)
				}
			} else {
				// crawled link is not one of my domains
				// ask server to figure out who should crawl this link
				// and have it return the ip of the worker who crawled
				// so I can add in the link in my own graph
				req := CrawlReq{
					URL:   link,
					Depth: depth - 1,
				}
				var res CrawlRes
				err = server.Call("MServer.Crawl", req, &res)
				checkError("", err, false)
				gl := GraphLink{
					URI:      link,
					WorkerIP: res.WorkerIP,
				}
				if !containsLink(workerGraph[uri], gl) {
					workerGraph[uri] = append(workerGraph[uri], gl)
					fmt.Fprintf(os.Stderr, "\nadding gl from %s to %#v\n", uri, gl)
				}
			}
			//fmt.Fprintf(os.Stderr, "added link, graph is %#v\n\n", workerGraph)
		}
	}

	*reply = MCrawlWebsiteRes{
		MyIP: myPubIP,
	}
	return nil
}

// MWorker.MeasureOverlap
// measure the overlap with another worker's graph
func (m *MWorker) MeasureOverlap(request OverlapReqWorker, reply *OverlapRes) error {
	uri1 := request.URL1
	uri2 := request.URL2
	workerIP2 := request.WorkerIP2
	uri1base, _ := url.Parse(uri1)
	uri1domain := uri1base.Host
	var linksToLookAt []GraphLink
	var subgraphLinks []string
	var otherSubgraph []string
	fmt.Fprintf(os.Stderr, "\nw1: current graph is %#v\n\n", workerGraph)

	// we own uri1 (or should)
	// pass along all url's in uri1's domain in uri1's subgraph
	uri1Links := workerGraph[uri1]
	subgraphLinks = append(subgraphLinks, uri1)
	linksToLookAt = append(linksToLookAt, uri1Links...)
	for len(linksToLookAt) > 0 {
		link := linksToLookAt[0].URI
		base, _ := url.Parse(link)
		// delete the link we are looking at from the list
		linksToLookAt = append(linksToLookAt[:0], linksToLookAt[1:]...)
		// ignore links not in our domain, or duplicates (cycle)
		if base.Host != uri1domain || contains(subgraphLinks, link) {
			continue
		} else {
			subgraphLinks = append(subgraphLinks, link)
			for _, sublink := range workerGraph[link] {
				if strings.Contains(sublink.URI, uri1domain) && !contains(subgraphLinks, sublink.URI) {
					linksToLookAt = append(linksToLookAt, sublink)
				}
			}
		}
	}
	fmt.Fprintf(os.Stderr, "w1: our subgraph links are %#v, will send\n", subgraphLinks)
	// pass subgraphLinks as an arg to worker 2
	worker2, err := rpc.Dial("tcp", workerIP2+":7369")
	checkError("", err, false)
	var res OverlapWithWorkerRes
	overlapReq := OverlapWithWorkerReq{
		Subgraph: subgraphLinks,
		URL:      uri2,
	}
	err = worker2.Call("MWorker.WorkerOverlap", overlapReq, &res)
	otherSubgraph = res.Subgraph
	fmt.Fprintf(os.Stderr, "w1:subgraph received was %#v\n", otherSubgraph)
	overlapCnt := 0
	for _, sublink := range subgraphLinks {
		for _, graphlink := range workerGraph[sublink] {
			//fmt.Fprintf(os.Stderr, "for %s, check if %s in %#v\n", sublink, graphlink.URI, otherSubgraph)
			if contains(otherSubgraph, graphlink.URI) {
				fmt.Fprintf(os.Stderr, "link from %s to %s found, and in %#v\n", sublink, graphlink.URI, otherSubgraph)
				overlapCnt++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "sum of overlaps is ours: %d plus theirs: %d\n", overlapCnt, res.NumPages)
	overlapCnt = overlapCnt + res.NumPages

	*reply = OverlapRes{
		NumPages: overlapCnt,
	}
	return nil
}

// MWorker.WorkerOverlap
// measure the overlap with another worker's graph given the other's subgraph
func (m *MWorker) WorkerOverlap(request OverlapWithWorkerReq, reply *OverlapWithWorkerRes) error {
	otherSubgraph := request.Subgraph
	uri2 := request.URL
	uri2base, _ := url.Parse(uri2)
	uri2domain := uri2base.Host
	var linksToLookAt []GraphLink
	var subgraphLinks []string
	fmt.Fprintf(os.Stderr, "\nw2: current graph is %#v\n\n", workerGraph)

	// we own uri2 (or should)
	// first figure out all url's in uri2's domain in uri2's subgraph
	uri2Links := workerGraph[uri2]
	subgraphLinks = append(subgraphLinks, uri2)
	linksToLookAt = append(linksToLookAt, uri2Links...)
	for len(linksToLookAt) > 0 {
		link := linksToLookAt[0].URI
		base, _ := url.Parse(link)
		linksToLookAt = append(linksToLookAt[:0], linksToLookAt[1:]...)
		if base.Host != uri2domain || contains(subgraphLinks, link) {
			continue
		} else {
			subgraphLinks = append(subgraphLinks, link)
			for _, sublink := range workerGraph[link] {
				if strings.Contains(sublink.URI, uri2domain) && !contains(subgraphLinks, sublink.URI) {
					linksToLookAt = append(linksToLookAt, sublink)
				}
			}
		}
	}
	fmt.Fprintf(os.Stderr, "our subgraph links are %#v, will return\n", subgraphLinks)
	// calculate overlap with the subgraph we were passed
	// ie worker 1's subgraph
	overlapCnt := 0
	for _, sublink := range subgraphLinks {
		for _, graphlink := range workerGraph[sublink] {
			//fmt.Fprintf(os.Stderr, "for %s, check if %s in %#v\n", sublink, graphlink.URI, otherSubgraph)
			if contains(otherSubgraph, graphlink.URI) {
				fmt.Fprintf(os.Stderr, "link from %s to %s found, and in %#v\n", sublink, graphlink.URI, otherSubgraph)
				overlapCnt++
			}
		}
	}

	// pass our subgraph plus the overlap count back to the callling worker
	*reply = OverlapWithWorkerRes{
		Subgraph: subgraphLinks,
		NumPages: overlapCnt,
	}
	return nil
}

func crawl(uri string) (links []string) {
	response, err := http.Get(uri)
	checkError("", err, false)
	// link dead, nothing to see here
	if err != nil {
		return links
	}
	body := response.Body
	defer body.Close()
	// much of the following code is adapted from
	// https://schier.co/blog/2015/04/26/a-simple-web-scraper-in-go.html
	z := html.NewTokenizer(body)
	for {
		tt := z.Next()
		switch {
		case tt == html.ErrorToken:
			// end of site document, exit loop
			body.Close()
			return links
		case tt == html.StartTagToken:
			t := z.Token()
			if t.Data == "a" {
				for _, a := range t.Attr {
					if a.Key == "href" {
						fmt.Println("Found href:", a.Val)
						if strings.HasSuffix(a.Val, ".html") {
							if !strings.HasPrefix(a.Val, "https://") && !strings.HasPrefix(a.Val, "ftp://") && !strings.HasPrefix(a.Val, "mailto:") {
								fmt.Println("legit link:", a.Val)
								links = append(links, a.Val)
							}
						}
						break
					}
				}
			}
		}
	}
}

func contains(arr []string, str string) bool {
	for _, el := range arr {
		if el == str {
			return true
		}
	}
	return false
}

func containsLink(arr []GraphLink, gl GraphLink) bool {
	for _, el := range arr {
		if el.URI == gl.URI {
			return true
		}
	}
	return false
}

// functions from previous assignments

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
