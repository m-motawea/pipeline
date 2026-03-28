package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/m-motawea/pipeline"
	"github.com/m-motawea/pipeline/amqpqueue"
	"github.com/m-motawea/pipeline/redisqueue"
)

func main() {
	driver := flag.String("driver", "redis", "Queue driver to use: redis or amqp")
	workerID := flag.String("worker", "A", "Worker ID for logging prefix")
	sendMsgs := flag.Bool("send", false, "Send test messages instead of running the pipeline worker loops")
	flag.Parse()

	log.SetPrefix(fmt.Sprintf("[Worker %s] ", *workerID))

	var queue pipeline.Queue
	var err error

	if *driver == "redis" {
		queue = redisqueue.NewRedisQueue(&redis.Options{
			Addr: "localhost:6379",
		})
		log.Println("Connected to Redis at localhost:6379")
	} else if *driver == "amqp" {
		queue, err = amqpqueue.NewAMQPQueue("amqp://guest:guest@localhost:5672/")
		if err != nil {
			log.Fatalf("Failed to connect to AMQP: %v", err)
		}
		log.Println("Connected to RabbitMQ at localhost:5672")
	} else {
		log.Fatalf("Unknown driver: %s", *driver)
	}

	var wg sync.WaitGroup
	pipe, _ := pipeline.NewPipeline("distributed_test", false, &wg, queue)

	if *sendMsgs {
		log.Println("Sending 5 test messages into the pipeline...")
		for i := 1; i <= 5; i++ {
			msg := pipeline.PipelineMessage{
				Direction: pipeline.PipelineInDirection,
				Content:   []byte{byte(i)},
			}
			pipe.SendMessage(msg)
			log.Printf("Sent message with initial value: %d", i)
		}
		log.Println("Done sending. Exiting.")
		os.Exit(0)
	}

	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			log.Printf("Proc '%s' (ID %d) received value: %d", proc.Name, proc.Id, val)
			msg.Content = []byte{byte(val * 10)}
			
			// Simulate slow complex processing mapping real world network requests
			time.Sleep(1500 * time.Millisecond)
			log.Printf("Proc '%s' (ID %d) finished processing. New value: %d", proc.Name, proc.Id, val*10)
		}
		return msg
	}

	proc1, _ := pipeline.NewPipelineProcess("stage_1", inFunc)
	proc2, _ := pipeline.NewPipelineProcess("stage_2", inFunc)

	pipe.AddProcess(&proc1)
	pipe.AddProcess(&proc2)

	log.Println("Starting Pipeline Workers. Waiting for messages...")
	pipe.Start()
}
