package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"github.com/awslabs/aws-sdk-go/service/sts"
	"io"
	"log"
	"net"
	"time"
)

// Customisable inputs.
// Memory used for metrics is pipeline size + send record size: at most (lineCap * pipelineCap) + recordCap [Bytes]
// Top throughput is no more than one record per minTick: at most recordCap / minTick [Bytes/second]
var (
	pipelineCap   = 1000
	lineCap       = 10 * 1024
	recordCap     = 50 * 1024
	maxTick       = 1000
	minTick       = 1000
	connHost      = "localhost"
	connPort      = 2003
	connType      = "tcp"
	maxRetries    = 3
	awsArn        = ""
	awsRegion     = ""
	awsStreamName = ""
)

// Computed inputs.
var (
	minTickDuration time.Duration
	maxTickDuration time.Duration
)

// AWS-related structures.
var (
	tempCredentials *tempCredentialsProvider
	kinesisClient   *kinesis.Kinesis
)

func init() {
	// Parse input parameters.
	flag.StringVar(&awsArn, "arn", "", "REQUIRED: ARN of the role to assume")
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

	flag.Parse()

	// Transform units from int (milliseconds) to duration.
	minTickDuration = time.Duration(minTick) * time.Millisecond
	maxTickDuration = time.Duration(maxTick) * time.Millisecond

	log.Printf("Peak possible memory usage for metrics in transit: %d MB\n", ((lineCap*pipelineCap)+recordCap)/1024/1024)
	log.Printf("Maximum Kinesis write throughput: %d kB/s\n", recordCap/minTick*1000/1024)

	// Validate required inputs.
	if awsArn == "" {
		log.Fatal("Provide ARN of the role to assume via --arn flag")
	}
	if awsRegion == "" {
		log.Fatal("Provide region via --region flag")
	}
	if awsStreamName == "" {
		log.Fatal("Provide Kinesis stream name via --stream-name flag")
	}

	// Create AWS structures.
	tempCredentials = &tempCredentialsProvider{}
	err := tempCredentials.Refresh()
	if err != nil {
		log.Fatal(err)
	}

	kinesisClient = kinesis.New(&aws.Config{
		Region:      awsRegion,
		Credentials: tempCredentials,
		MaxRetries:  maxRetries,
	})
	kinesisClient.ShouldRetry = shouldRetry
}

func main() {
	pipeline := make(chan []byte, pipelineCap)
	go listener(pipeline)
	sender(pipeline)
}

type tempCredentialsProvider struct {
	Role *sts.AssumeRoleOutput
}

// Refresh the temporary credentials - get a new role.
func (p *tempCredentialsProvider) Refresh() error {
	stsClient := sts.New(&aws.Config{
		Region:     awsRegion,
		MaxRetries: maxRetries,
	})

	var err error
	p.Role, err = stsClient.AssumeRole(&sts.AssumeRoleInput{
		RoleARN:         aws.String(awsArn),
		RoleSessionName: aws.String("kinesis"),
	})

	return err
}

// Transforms the temporary sts.Credentials stored in the role into proper aws.Credentials.
func (p *tempCredentialsProvider) Credentials() (*aws.Credentials, error) {
	if p.Role == nil {
		err := p.Refresh()
		if err != nil {
			return nil, err
		}
	}

	return &aws.Credentials{
		AccessKeyID:     *p.Role.Credentials.AccessKeyID,
		SecretAccessKey: *p.Role.Credentials.SecretAccessKey,
		SessionToken:    *p.Role.Credentials.SessionToken,
	}, nil
}

// Check if the request should be retried with exponential backoff.
// Copied mostly from github.com/awslabs/aws-sdk-go/aws/service.go
func shouldRetry(r *aws.Request) bool {
	if r.HTTPResponse.StatusCode >= 500 {
		return true
	} else if err := aws.Error(r.Error); err != nil {
		switch err.Code {
		case "ExpiredTokenException":
			// Fine to retry, but only after refreshing credentials.
			log.Println("Token has expired. Refreshing and retrying...")
			err := tempCredentials.Refresh()
			if err != nil {
				return false
			}
			return true
		case "ProvisionedThroughputExceededException", "Throttling":
			log.Println("Hit throttling exception. Backing off...")
			return true
		}
	}
	return false
}

// Sends the record to Kinesis. This could take any amount of time due to the internal retries.
func sendAndReset(record *bytes.Buffer) {
	output, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{
		Data:         record.Bytes(),
		PartitionKey: aws.String("1"),
		StreamName:   aws.String(awsStreamName),
	})
	if err != nil {
		log.Println(err)
	} else {
		log.Printf("Sent %d bytes with Seqno %s and ShardID %s\n", record.Len(), *output.SequenceNumber, *output.ShardID)
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
			record.Write(line)
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

			// Returns err != nil if and only if the returned data does not end in delim.
			if err != nil {
				if err != io.EOF {
					log.Println(err)
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
