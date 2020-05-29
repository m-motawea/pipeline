package pipeline

import (
	"log"
	"sync"
	"testing"
	"time"
)

func TestOneWayPipeline(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc PipelineProcess, msg PipelineMessage) PipelineMessage {
		log.Println("inFunc received msg")
		val, ok := msg.Content.(int)
		if ok {
			if val != proc.Id {
				t.Errorf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = val + 1
		} else {
			t.Errorf("Message Content is not int")
		}
		return msg
	}
	pipe, _ := NewPipeline("test_pipeline", false, &wg)
	msg := PipelineMessage{
		Direction: PipelineInDirection{},
		Content:   0,
	}

	proc0, _ := NewPipelineProcess("test_proc1", inFunc)
	proc1, _ := NewPipelineProcess("test_proc2", inFunc)
	proc2, _ := NewPipelineProcess("test_proc3", inFunc)
	pipe.AddProcess(&proc0)
	pipe.AddProcess(&proc1)
	pipe.AddProcess(&proc2)
	go pipe.Start()
	time.Sleep(2 * time.Second)
	log.Println("Pipeline started")
	pipe.SendMessage(msg)
	for i := 0; i < 20; i++ {
		pipe.SendMessage(msg)
	}
	// time.Sleep(10 * time.Second)
	go func() {
		time.Sleep(5 * time.Second)
		pipe.Stop()
	}()
	wg.Wait()

}

func TestOneWayPipelineConsumer(t *testing.T) {
	var wg sync.WaitGroup
	inFunc := func(proc PipelineProcess, msg PipelineMessage) PipelineMessage {
		log.Println("inFunc received msg")
		val, ok := msg.Content.(int)
		if ok {
			if val != proc.Id {
				t.Errorf("msg content value: %d != proc.Id: %d", val, proc.Id)
			}
			msg.Content = val + 1
		} else {
			t.Errorf("Message Content is not int")
		}
		return msg
	}

	consumeChannel := make(PipelineChannel)
	pipe, _ := NewPipeline("test_pipeline", false, &wg, consumeChannel)
	msg := PipelineMessage{
		Direction: PipelineInDirection{},
		Content:   0,
	}

	proc0, _ := NewPipelineProcess("test_proc1", inFunc)
	proc1, _ := NewPipelineProcess("test_proc2", inFunc)
	proc2, _ := NewPipelineProcess("test_proc3", inFunc)
	pipe.AddProcess(&proc0)
	pipe.AddProcess(&proc1)
	pipe.AddProcess(&proc2)
	go pipe.Start()
	time.Sleep(2 * time.Second)
	log.Println("Pipeline started")
	go func() {
		i := 0
		for i < 19 {
			log.Println("Waiting for pipe result")
			select {
			case <-pipe.consumerChannel:
				log.Printf("consumer received message %d", i)
				i += 1
			}
		}
		pipe.Stop()
	}()
	pipe.SendMessage(msg)
	for i := 0; i < 20; i++ {
		pipe.SendMessage(msg)
	}
	// time.Sleep(10 * time.Second)
	wg.Wait()
}
