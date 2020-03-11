package pipeline

import (
	"log"
	"testing"
)

func TestPipelineProcess(t *testing.T) {
	inFunc := func(proc PipelineProcess, msg PipelineMessage) PipelineMessage {
		val, ok := msg.Content.(int)
		if ok {
			msg.Content = val + 1
		} else {
			t.Error("Message Content is not int")
		}
		return msg
	}

	outChannel := make(PipelineChannel)

	proc, _ := NewPipelineProcess("test_proc", inFunc)
	msg := PipelineMessage{
		Direction: PipelineInDirection{},
		Content:   0,
	}
	go proc.InProcess(outChannel)
	for i := 0; i < 5; i++ {
		log.Printf("sending msg %+v", msg)
		proc.InQueue(msg)
		log.Println("Waiting for result..")
		res := <-outChannel
		log.Printf("Got result %+v", res)
		val, ok := res.Content.(int)
		if ok {
			if val != i+1 {
				t.Errorf("msg Content: %d != %d", val, i+1)
			}
		} else {
			t.Errorf("Invalid msg Content")
		}
		msg = res
	}
	proc.Close()
}
