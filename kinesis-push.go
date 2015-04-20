package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const recordCap = 50 * 1024

func main() {
	pipeName := "kinesis-push"

	// Create a named pipe for input.
	if _, err := os.Stat(pipeName); os.IsExist(err) {
		log.Fatal(err)
	}

	if err := syscall.Mknod(pipeName, syscall.S_IFIFO|0660, 0); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(pipeName)

	// Catch signals and quit gracefully.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		_ = <-sigChan
		// This allows us to clean up.
		os.Remove(pipeName)
		os.Exit(1)
	}()

	readDaemon(pipeName)
}

func readDaemon(pipeName string) {
	// This will block until the first byte is available.
	pipe, err := os.Open(pipeName)
	if err != nil {
		panic(err)
	}
	defer pipe.Close()

	pipeReader := bufio.NewReader(pipe)
	// Buffer to hold a single record. Kinesis can handle up to 50kB of data in one go.
	record := new(bytes.Buffer)
	// Buffer for a single line of metrics.
	var line []byte

	for {
		// Keep reading until we have drained the buffer, or reached 50kB record limit.
		for {
			var err error
			if len(line) == 0 {
				line, err = pipeReader.ReadBytes('\n')
			}

			// Ignore EOF, processes will write data to the pipe many times.
			if err != nil && err != io.EOF {
				panic(err)
			}

			// If there is no data left, flush the buffer.
			if len(line) == 0 {
				break
			}

			// We definitely cannot send a line over 50kB - it won't fit in a single record.
			// With metrics, it's fairly unlikely to have a line that long, so we truncate here just to be sure.
			if len(line) > recordCap {
				line = line[0 : recordCap-1]
			}

			// Check for overflow. The "line" buffer gets passed to the next iteration.
			if len(line)+record.Len() > recordCap {
				break
			}

			// Flush the line and clear the temporary line buffer.
			record.Write(line)
			line = nil

		}

		// Send the record.
		if record.Len() > 0 {
			log.Printf("Sending %d bytes\n", record.Len())
			record.Reset()
		}

		// Wait for more data.
		time.Sleep(5 * time.Second)
	}
}
