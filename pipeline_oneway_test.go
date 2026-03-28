package pipeline_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/m-motawea/pipeline"
	"github.com/m-motawea/pipeline/localqueue"
)

func TestOneWayPipeline(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		log.Println("inFunc received msg")
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			if val != proc.Id {
				t.Errorf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = []byte{byte(val + 1)}
		} else {
			t.Errorf("Message Content is empty")
		}
		return msg
	}
	
	queue := localqueue.NewLocalQueue()
	pipe, _ := pipeline.NewPipeline("test_pipeline", false, &wg, queue)
	msg := pipeline.PipelineMessage{
		Direction: pipeline.PipelineInDirection,
		Content:   []byte{0},
	}

	proc0, _ := pipeline.NewPipelineProcess("test_proc1", inFunc)
	proc1, _ := pipeline.NewPipelineProcess("test_proc2", inFunc)
	proc2, _ := pipeline.NewPipelineProcess("test_proc3", inFunc)
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

func TestOneWayPipelineConsumer(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		log.Println("inFunc received msg")
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			if val != proc.Id {
				t.Errorf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = []byte{byte(val + 1)}
		} else {
			t.Errorf("Message Content is empty")
		}
		return msg
	}

	queue := localqueue.NewLocalQueue()
	consumeChannel := make(pipeline.PipelineChannel)
	pipe, _ := pipeline.NewPipeline("test_pipeline_consumer", false, &wg, queue, consumeChannel)
	msg := pipeline.PipelineMessage{
		Direction: pipeline.PipelineInDirection,
		Content:   []byte{0},
	}

	proc0, _ := pipeline.NewPipelineProcess("test_proc1", inFunc)
	proc1, _ := pipeline.NewPipelineProcess("test_proc2", inFunc)
	proc2, _ := pipeline.NewPipelineProcess("test_proc3", inFunc)
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
		for i < 21 { // we send 1 + 20 messages
			log.Println("Waiting for pipe result")
			select {
			// consumeChannel is internal or returned
			// wait, consumerChannel is not exported!
			case <-consumeChannel:
				log.Printf("consumer received message %d", i)
				i += 1
			case <-time.After(10 * time.Second):
                log.Println("Timout waiting for consumer channel")
                i = 21 // Exit loop on timeout
			}
		}
		pipe.Stop()
		consumerWg.Done()
	}()
	pipe.SendMessage(msg)
	for i := 0; i < 20; i++ {
		pipe.SendMessage(msg)
	}
	consumerWg.Wait() // wait for consumer to finish before wg.Wait
	wg.Wait()
}
