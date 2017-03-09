package main

import (
    "net/http"
    "os"
    "log"
    "fmt"
    "io/ioutil"
)

func handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(os.Stdout, "%s details: %+v\n", r.URL.Path,r)
    buf, err := ioutil.ReadFile("."+r.URL.Path+"/index.html")
    if err != nil {
        fmt.Fprintf(w, "There does not seem to be a page at %s sorry",r.URL.Path)
    }
    fmt.Fprintf(w, "%s",string(buf))
}

//Usage go run server.go [public ip]
//default port is 8080 for all http requests
func main() {
    if len(os.Args) != 2 {
        log.Fatal("please pass in an ip as an argument")
    }
    ip := os.Args[1]
    log.Printf("Hey 416! I'm a running webserver")
    log.Fatal(http.ListenAndServe(ip+":80", http.HandlerFunc(handler)))
}


