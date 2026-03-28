package pipeline_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-motawea/pipeline"
	"github.com/m-motawea/pipeline/localqueue"
)

func TestTwoWayPipeline(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			if val != proc.Id {
				log.Fatalf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = []byte{byte(val + 1)}
		} else {
			log.Fatalf("Message Content is empty")
		}
		return msg
	}
	outFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			msg.Content = []byte{byte(val - 1)}
			if val-1 != proc.Id {
				log.Fatalf("msg content value: %d != proc.Id: %d", val-1, proc.Id)
			}
		} else {
			log.Fatalf("Message Content is empty")
		}
		return msg
	}

	queue := localqueue.NewLocalQueue()
	pipe, _ := pipeline.NewPipeline("test_twoway_pipeline", true, &wg, queue)
	msg := pipeline.PipelineMessage{
		Direction: pipeline.PipelineInDirection,
		Content:   []byte{0},
	}

	proc0, _ := pipeline.NewPipelineProcess("test_proc1", inFunc, outFunc)
	proc1, _ := pipeline.NewPipelineProcess("test_proc2", inFunc, outFunc)
	proc2, _ := pipeline.NewPipelineProcess("test_proc3", inFunc, outFunc)
	pipe.AddProcess(&proc0)
	pipe.AddProcess(&proc1)
	pipe.AddProcess(&proc2)
	go pipe.Start()
	time.Sleep(1 * time.Second)
	log.Println("Pipeline started")

	pipe.SendMessage(msg)
	for i := 0; i < 20; i++ {
		pipe.SendMessage(msg)
	}

	go func() {
		time.Sleep(3 * time.Second)
		pipe.Stop()
	}()
	wg.Wait()
}

func TestTwoWayPipelineConsumer(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			if val != proc.Id {
				log.Fatalf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = []byte{byte(val + 1)}
		} else {
			log.Fatalf("Message Content is empty")
		}
		return msg
	}
	outFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			msg.Content = []byte{byte(val - 1)}
			if val-1 != proc.Id {
				log.Fatalf("msg content value: %d != proc.Id: %d", val-1, proc.Id)
			}
		} else {
			log.Fatalf("Message Content is empty")
		}
		return msg
	}

	queue := localqueue.NewLocalQueue()
	consumeChannel := make(pipeline.PipelineChannel)
	pipe, _ := pipeline.NewPipeline("test_twoway_pipeline_consumer", true, &wg, queue, consumeChannel)
	msg := pipeline.PipelineMessage{
		Direction: pipeline.PipelineInDirection,
		Content:   []byte{0},
	}

	proc0, _ := pipeline.NewPipelineProcess("test_proc1", inFunc, outFunc)
	proc1, _ := pipeline.NewPipelineProcess("test_proc2", inFunc, outFunc)
	proc2, _ := pipeline.NewPipelineProcess("test_proc3", inFunc, outFunc)
	pipe.AddProcess(&proc0)
	pipe.AddProcess(&proc1)
	pipe.AddProcess(&proc2)
	go pipe.Start()
	time.Sleep(1 * time.Second)
	log.Println("Pipeline started")

	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		i := 0
		for i < 21 { // we sent 1 + 20
			log.Println("Waiting for pipe result")
			select {
			case <-consumeChannel:
				log.Printf("consumer received message %d", i)
				i += 1
			case <-time.After(10 * time.Second):
                log.Println("Timout waiting for consumer channel")
                i = 21
			}
		}
		pipe.Stop()
		consumerWg.Done()
	}()
	pipe.SendMessage(msg)
	for i := 0; i < 20; i++ {
		pipe.SendMessage(msg)
	}

	consumerWg.Wait()
	wg.Wait()
}
