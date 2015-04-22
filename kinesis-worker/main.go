package main

import (
	"flag"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"github.com/mateusz/aws-temp-creds"
	"log"
	"net"
	"os"
	"time"
)

// Customisable inputs.
var (
	tick            = 10000
	getRecordsLimit = 10000
	connHost        = "localhost"
	connPort        = 2003
	connType        = "tcp"
	maxRetries      = 3
	awsArn          = ""
	awsRegion       = ""
	awsStreamName   = ""
	credDuration    = 3600
	tee             = false
)

// Computed inputs.
var (
	tickDuration time.Duration
)

func init() {
	// Parse input parameters.
	flag.StringVar(&awsArn, "arn", "", "ARN of the role to assume")
	flag.StringVar(&awsRegion, "region", "", "REQUIRED: Region")
	flag.StringVar(&awsStreamName, "stream-name", "", "REQUIRED: Kinesis stream name")
	flag.IntVar(&tick, "tick", tick, "Seconds to elapse between subsequent get requests. Shards are processed in parallel.")
	flag.IntVar(&getRecordsLimit, "get-records-limit", getRecordsLimit, "Maximum amount of records to fetch within on get records operation.")
	flag.StringVar(&connHost, "host", connHost, "Hostname")
	flag.IntVar(&connPort, "port", connPort, "Port")
	flag.StringVar(&connType, "protocol", connType, "Protocol (tcp or udp)")
	flag.IntVar(&maxRetries, "max-retries", maxRetries, "Amount of times to retry if AWS API calls fail.")
	flag.IntVar(&credDuration, "cred-duration", credDuration, "Temporary credentials duration. Min 900, max 3600 seconds.")
	flag.BoolVar(&tee, "tee", tee, "Print data being sent to the network to stdout.")

	flag.Parse()

	// Transform units from int (milliseconds) to duration.
	tickDuration = time.Duration(tick) * time.Millisecond

	// Validate required inputs.
	if awsRegion == "" {
		log.Fatal("Provide region via --region flag")
	}
	if awsStreamName == "" {
		log.Fatal("Provide Kinesis stream name via --stream-name flag")
	}

}

func main() {
	receiver()
}

// Create new client.
func newKinesisClient() *kinesis.Kinesis {
	if awsArn != "" {
		tempCredentials := &awstempcreds.TempCredentialsProvider{
			Region:   awsRegion,
			Duration: time.Duration(credDuration) * time.Second,
			RoleARN:  awsArn,
		}

		return kinesis.New(&aws.Config{
			Region:      awsRegion,
			Credentials: tempCredentials,
			MaxRetries:  maxRetries,
		})
	} else {
		return kinesis.New(&aws.Config{
			Region:     awsRegion,
			MaxRetries: maxRetries,
		})
	}
}

// ShardReceiver continuously reads records from Kinesis and passes them onto a network connection.
func shardReceiver(quit chan bool, shardId *string) {
	var shardIterator *string
	var nextTick time.Time

	// One client per thread - the aws-temp-creds is not concurrency-safe.
	client := newKinesisClient()

	for {
		// Wait until the tick elapses.
		now := time.Now()
		if now.Before(nextTick) {
			time.Sleep(nextTick.Sub(now))
		}
		nextTick = time.Now().Add(tickDuration)

		// Initialise shard iterator, if we don't have a valid one.
		if shardIterator == nil {
			shardIteratorOutput, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
				ShardID:           shardId,
				ShardIteratorType: aws.String("LATEST"),
				StreamName:        aws.String(awsStreamName),
			})
			if err != nil {
				// Let's retry next time around.
				log.Printf("ShardReceiver failed to get the iterator: %s\n", err)
				continue
			}
			shardIterator = shardIteratorOutput.ShardIterator
		}

		// Get data from Kinesis.
		recordsOutput, err := client.GetRecords(&kinesis.GetRecordsInput{
			Limit:         aws.Long(int64(getRecordsLimit)),
			ShardIterator: shardIterator,
		})
		if err != nil {
			log.Printf("ShardReceiver failed to get records: %s\n", err)
			switch err := err.(type) {
			case aws.APIError:
				switch err.Code {
				case "ExpiredIteratorException":
					// Force iterator refresh.
					log.Println("Forcing iterator refresh.")
					shardIterator = nil
					continue
				}
			}

			continue
		}

		// Use the new iterator provided - they expire in 300s.
		shardIterator = recordsOutput.NextShardIterator

		if len(recordsOutput.Records) > 0 {
			if tee {
				for _, record := range recordsOutput.Records {
					fmt.Fprintf(os.Stdout, "%s", record.Data)
				}
			}

			dialer, err := net.Dial(connType, fmt.Sprintf("%s:%d", connHost, connPort))
			if err != nil {
				log.Printf("ShardReceiver failed to connect to the downstream: %s\n", err)
				continue
			}

			for _, record := range recordsOutput.Records {
				dialer.Write(record.Data)
			}

			dialer.Close()
		}

	}
}

// Receiver kicks off a shardReceiver for each shard and blocks until the quit signal is received.
func receiver() {
	client := newKinesisClient()
	stream, err := client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(awsStreamName),
	})
	if err != nil {
		log.Printf("Receiver failed to describe the stream: %s\n", err)
		return
	}
	client = nil

	quit := make(chan bool)
	for _, shard := range stream.StreamDescription.Shards {
		go shardReceiver(quit, shard.ShardID)
	}

	// Wait for the first quit signal.
	<-quit
}
