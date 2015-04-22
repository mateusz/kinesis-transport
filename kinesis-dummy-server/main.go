package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

var (
	connHost = "localhost"
	connPort = 2003
	connType = "tcp"
)

func init() {
	flag.StringVar(&connHost, "host", connHost, "Hostname")
	flag.IntVar(&connPort, "port", connPort, "Port")
	flag.StringVar(&connType, "protocol", connType, "Protocol (tcp or udp)")

	flag.Parse()
}

func main() {
	listener()
}

func listener() {
	listener, err := net.Listen(connType, fmt.Sprintf("%s:%d", connHost, connPort))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Listener failed to accept connection: %s\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Send lines down the channel.
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadBytes('\n')

			// Returns err != nil if and only if the returned data does not end in delim.
			if err != nil {
				if err != io.EOF {
					log.Printf("Listener failed reading from connection: %s\n", err)
				}
				break
			}

			log.Printf("%s", line)

		}

		conn.Close()
	}
}
