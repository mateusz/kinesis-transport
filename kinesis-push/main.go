package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"github.com/mateusz/aws-temp-creds"
	"io"
	"log"
	"net"
	"os"
	"time"
)

// Customisable inputs.
// Memory used for metrics is pipeline size + send record size: at most (lineCap * pipelineCap) + recordCap [Bytes]
// Top throughput is no more than one record per minTick: at most recordCap / minTick [Bytes/second]
var (
	pipelineCap   = 1000
	lineCap       = 10 * 1024
	recordCap     = 50 * 1024
	maxTick       = 10000
	minTick       = 10000
	connHost      = "localhost"
	connPort      = 2003
	connType      = "tcp"
	maxRetries    = 3
	awsArn        = ""
	awsRegion     = ""
	awsStreamName = ""
	credDuration  = 3600
	tee           = false
)

// Computed inputs.
var (
	minTickDuration time.Duration
	maxTickDuration time.Duration
)

// AWS-related structures.
var (
	kinesisClient *kinesis.Kinesis
)

func init() {
	// Parse input parameters.
	flag.StringVar(&awsArn, "arn", "", "ARN of the role to assume")
	flag.StringVar(&awsRegion, "region", "", "REQUIRED: Region")
	flag.StringVar(&awsStreamName, "stream-name", "", "REQUIRED: Kinesis stream name")
	flag.IntVar(&pipelineCap, "pipeline-cap", pipelineCap, "Amount of metric lines we can hold in the buffer at any given time. Overflow will be immediately be dropped.")
	flag.IntVar(&lineCap, "line-cap", lineCap, "Maximum length of a single metric line, before truncation occurs.")
	flag.IntVar(&recordCap, "record-cap", recordCap, "Maximum size of the record to send. Do not set to more than 50kB - that's maximum permitted by Kinesis.")
	flag.IntVar(&maxTick, "max-tick", maxTick, "Maximum amount of milliseconds to wait for data before sending a single record. This is how much data we can stand to loose if the daemon crashes. Excludes send time.")
	flag.IntVar(&minTick, "min-tick", minTick, "Minimum amount of milliseconds between subsequent send attempts. Must be < max-tick, excludes send time.")
	flag.StringVar(&connHost, "host", connHost, "Hostname")
	flag.IntVar(&connPort, "port", connPort, "Port")
	flag.StringVar(&connType, "protocol", connType, "Protocol (tcp or udp)")
	flag.IntVar(&maxRetries, "max-retries", maxRetries, "Amount of times to retry if AWS API calls fail - used twice: both internally within the AWS calls and externally.")
	flag.IntVar(&credDuration, "cred-duration", credDuration, "Temporary credentials duration. Min 900, max 3600 seconds.")
	flag.BoolVar(&tee, "tee", tee, "Print data being sent to Kinesis to stdout.")

	flag.Parse()

	// Transform units from int (milliseconds) to duration.
	minTickDuration = time.Duration(minTick) * time.Millisecond
	maxTickDuration = time.Duration(maxTick) * time.Millisecond

	// Validate required inputs.
	if awsRegion == "" {
		log.Fatal("Provide region via --region flag")
	}
	if awsStreamName == "" {
		log.Fatal("Provide Kinesis stream name via --stream-name flag")
	}

	// Create AWS structures.
	if awsArn != "" {
		tempCredentials := &awstempcreds.TempCredentialsProvider{
			Region:   awsRegion,
			Duration: time.Duration(credDuration) * time.Second,
			RoleARN:  awsArn,
		}

		kinesisClient = kinesis.New(&aws.Config{
			Region:      awsRegion,
			Credentials: tempCredentials,
			MaxRetries:  maxRetries,
		})
	} else {
		kinesisClient = kinesis.New(&aws.Config{
			Region:     awsRegion,
			MaxRetries: maxRetries,
		})
	}
}

func main() {
	pipeline := make(chan []byte, pipelineCap)
	go listener(pipeline)
	sender(pipeline)
}

// Sends the record to Kinesis. This could take any amount of time due to the internal retries.
func sendAndReset(record *bytes.Buffer) {
	if tee {
		fmt.Fprint(os.Stdout, record.String())
	}

	// Partition by hashing the data. This will be a bit random, but will at least ensure all shards are used
	// (if we ever have more than one)
	partitionKey := fmt.Sprintf("%x", md5.Sum(record.Bytes()))

	// Try a few times on error. The initial reason for this is Go AWS SDK seems to have some weird timing issue,
	// where sometimes the request would just EOF if requests are made in regular intervals. For example doing
	// "put-record" from us-west-1 to ap-southeast-2 every 6-7 seconds will cause EOF error, without the record being sent.
	for i, backoff := 0, time.Second; i < maxRetries; i, backoff = i+1, backoff*2 {
		_, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{
			Data:         record.Bytes(),
			PartitionKey: aws.String(partitionKey),
			StreamName:   aws.String(awsStreamName),
		})

		if err == nil {
			break
		}

		// Send has failed.
		if i < maxRetries-1 {
			log.Printf("Retrying in %d s, sender failed to put record on try %d: %s.\n", backoff/time.Second, i, err)
			time.Sleep(backoff)
		} else {
			log.Printf("Aborting, sender failed to put record on try %d: %s.\n", i, err)
		}
	}

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
				if sinceStart < minTickDuration {
					time.Sleep(minTickDuration - sinceStart)
				}

				sendAndReset(record)
				tickStart = time.Now()
			}

			_, err := record.Write(line)
			if err != nil {
				log.Fatal("Sender failed to write into the record buffer: %s\n", err)
			}
		default:
			// Poll for some more data to come.
			time.Sleep(1 * time.Second)
		}

		// Wait for data for maxTick, then send what we have.
		if time.Since(tickStart) > maxTickDuration {
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

			// Truncate lines longer than lineCap size.
			if len(line) > lineCap {
				line = line[0:lineCap]
				line[lineCap-1] = '\n'
			}

			// If we have overflown the channel, drop data on the floor.
			if len(pipeline) == cap(pipeline) {
				continue
			}

			pipeline <- line

		}

		conn.Close()
	}
}
