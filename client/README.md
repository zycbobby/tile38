Tile38 Client
=============

[![Build Status](https://travis-ci.org/tidwall/tile38.svg?branch=master)](https://travis-ci.org/tidwall/tile38)
[![GoDoc](https://godoc.org/github.com/tidwall/tile38/client?status.svg)](https://godoc.org/github.com/tidwall/tile38/client)

Tile38 Client is a [Go](http://golang.org/) client for [Tile38](http://tile38.com/).

## Examples

#### Connection
```go
package main

import "github.com/tidwall/tile38/client"

func main(){
    conn, err := client.Dial("localhost:9851")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    resp, err := conn.Do("set fleet truck1 point 33.5123 -112.2693")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(resp))
}

```

#### Pool
```go
package main

import "github.com/tidwall/tile38/client"

func main(){
    pool, err := client.DialPool("localhost:9851")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // We'll set a point in a background routine
    go func() {
        conn, err := pool.Get() // get a conn from the pool
        if err != nil {
            log.Fatal(err)
        }
        defer conn.Close() // return the conn to the pool
        _, err = conn.Do("set fleet truck1 point 33.5123 -112.2693")
        if err != nil {
            log.Fatal(err)
        }
    }()
    time.Sleep(time.Second / 2) // wait a moment

    // Retreive the point we just set.
    go func() {
        conn, err := pool.Get() // get a conn from the pool
        if err != nil {
            log.Fatal(err)
        }
        defer conn.Close() // return the conn to the pool
        resp, err := conn.Do("get fleet truck1 point")
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println(string(resp))
    }()
    time.Sleep(time.Second / 2) // wait a moment
}
```