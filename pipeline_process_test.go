package pipeline_test

import (
	"context"
	"log"
	"sync"
	"testing"

	"github.com/m-motawea/pipeline"
	"github.com/m-motawea/pipeline/localqueue"
)

func TestPipelineProcess(t *testing.T) {
	inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
		if len(msg.Content) > 0 {
			val := int(msg.Content[0])
			msg.Content = []byte{byte(val + 1)}
		} else {
			t.Error("Message Content is empty")
		}
		return msg
	}

	queue := localqueue.NewLocalQueue()
	var wg sync.WaitGroup
	pipe, _ := pipeline.NewPipeline("test_proc_pipeline", false, &wg, queue)

	proc, _ := pipeline.NewPipelineProcess("test_proc", inFunc)
	pipe.AddProcess(&proc) // Adds process and registers Id

	msg := pipeline.PipelineMessage{
		Direction: pipeline.PipelineInDirection,
		Content:   []byte{0},
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	go proc.InProcess(ctx, queue)

	for i := 0; i < 5; i++ {
		log.Printf("sending msg %+v", msg)
		proc.InQueue(msg) // publish to proc channel
		log.Println("Waiting for result..")
		
		ch, _ := queue.Subscribe(ctx, "test_proc_pipeline_orchestrator", "test")
		res := <-ch
		
		log.Printf("Got result %+v", res)
		if len(res.Content) > 0 {
			val := int(res.Content[0])
			if val != i+1 {
				t.Errorf("msg Content: %d != %d", val, i+1)
			}
		} else {
			t.Errorf("Invalid msg Content")
		}
		msg = res
		msg.LastProcess = -1
	}
	cancel() // Stops the process loop safely
}
