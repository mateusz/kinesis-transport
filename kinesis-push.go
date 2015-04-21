package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"time"
)

// Memory used for metrics is pipeline size + send record size: at most (lineCap * pipelineCap) + recordCap [Bytes]
// Top throughput is no more than one record per minTick: at most recordCap / minTick [Bytes/second]
const (
	// Amount of metric lines we can hold in the buffer at any given time. Overflow will immediately be dropped.
	pipelineCap = 1000
	// Maximum length of a single metric line.
	lineCap = 10 * 1024
	// Maximum size of the record to send.
	recordCap = 50 * 1024
	// Maximum amount of time to wait for data before sending a single record.
	// This is how much data we can stand to loose if the daemon crashes.
	maxTick = 1 * time.Second
	// Minimum amount of time between subsequent sends.
	// Must be < maxTick
	minTick = 1 * time.Second
	// TCP connection details
	connHost = "localhost"
	connPort = "2003"
	connType = "tcp"
)

func main() {
	pipeline := make(chan []byte, pipelineCap)
	go listener(pipeline)
	sender(pipeline)
}

// Sends the record to Kinesis.
func sendAndReset(record *bytes.Buffer) {
	log.Printf("Sending %d\n", record.Len())
	record.Reset()
}

// Continuously drains the pipeline, waits for data for maxTick, sends not more frequently than minTick.
func sender(pipeline chan []byte) {
	record := new(bytes.Buffer)
	tickStart := time.Now()
	for {
		select {
		case line := <-pipeline:
			// Check if we are overflowing the 50kB maximum Kinesis message size.
			if len(line)+record.Len() > recordCap {
				// Respect the minTick - we don't want to send more often than once per minTick.
				sinceStart := time.Since(tickStart)
				if sinceStart < minTick {
					time.Sleep(minTick - sinceStart)
				}

				sendAndReset(record)
				tickStart = time.Now()
			}
			record.Write(line)
		default:
			// Poll for some more data to come.
			time.Sleep(1 * time.Second)
		}

		// Wait for data for maxTick, then send what we have.
		if time.Since(tickStart) > maxTick {
			if record.Len() > 0 {
				sendAndReset(record)
			}
			tickStart = time.Now()
		}
	}
}

// Listens to TCP port and puts all the data into the pipeline channel.
// Lines are truncated to lineCap, if the channel has no more space remaining lines are dropped.
func listener(pipeline chan []byte) {
	listener, err := net.Listen(connType, connHost+":"+connPort)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Send lines down the channel.
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				log.Println(err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// If there is no data left, finish reading.
			if len(line) == 0 {
				break
			}

			// Truncate lines longer than lineCap size.
			if len(line) > lineCap {
				line = line[0:lineCap]
				line[lineCap-1] = '\n'
			}

			// If we have overflown the channel, drop data on the floor.
			if len(pipeline) < cap(pipeline) {
				pipeline <- line
			}

		}

		conn.Close()
	}
}
