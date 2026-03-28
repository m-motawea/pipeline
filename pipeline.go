package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type PipelineDirection string

const (
	PipelineInDirection  PipelineDirection = "IN"
	PipelineOutDirection PipelineDirection = "OUT"
)

type PipelineMessage struct {
	LastProcess int
	Direction   PipelineDirection
	Content     []byte
	Finished    bool
	Drop        bool
}

type PipelineChannel chan PipelineMessage

type PipelineProcess struct {
	Id          int
	Name        string
	inProcess   func(PipelineProcess, PipelineMessage) PipelineMessage
	outProcess  func(PipelineProcess, PipelineMessage) PipelineMessage
	Concurrency int
	Pipe        *Pipeline
}

func (proc *PipelineProcess) InProcess(ctx context.Context, queue Queue) {
	log.Printf("Process: Starting InProcess for %s id: %d", proc.Name, proc.Id)
	topic := fmt.Sprintf("%s_proc_%d_in", proc.Pipe.Name, proc.Id)
	ch, err := queue.Subscribe(ctx, topic, proc.Pipe.consumerGroup)
	if err != nil {
		log.Printf("Process %s failed to subscribe InProcess: %v", proc.Name, err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("InProcess: Closing.")
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			log.Printf("Process %s: InProcess: Received Message %+v", proc.Name, msg)
			res := proc.inProcess(*proc, msg)
			if res.Drop {
				continue
			}
			res.LastProcess = proc.Id
			log.Printf("Process %s: InProcess: inFunc Result %+v", proc.Name, res)
			orchestratorTopic := fmt.Sprintf("%s_orchestrator", proc.Pipe.Name)
			queue.Publish(ctx, orchestratorTopic, res)
		}
	}
}

func (proc *PipelineProcess) OutProcess(ctx context.Context, queue Queue) {
	log.Printf("Process: Starting OutProcess for %s id: %d", proc.Name, proc.Id)
	topic := fmt.Sprintf("%s_proc_%d_out", proc.Pipe.Name, proc.Id)
	ch, err := queue.Subscribe(ctx, topic, proc.Pipe.consumerGroup)
	if err != nil {
		log.Printf("Process %s failed to subscribe OutProcess: %v", proc.Name, err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Printf("Process %s: OutProcess: Closing.", proc.Name)
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			log.Printf("Process %s: OutProcess: Received Message %+v", proc.Name, msg)
			res := proc.outProcess(*proc, msg)
			if res.Drop {
				continue
			}
			res.LastProcess = proc.Id
			log.Printf("Process %s: OutProcess: inFunc Result %+v", proc.Name, res)
			orchestratorTopic := fmt.Sprintf("%s_orchestrator", proc.Pipe.Name)
			queue.Publish(ctx, orchestratorTopic, res)
		}
	}
}

func (proc *PipelineProcess) Close() {
	// Handled by Pipeline context cancellation
}

func (proc *PipelineProcess) InQueue(msg PipelineMessage) {
	log.Printf("Process %d: InQueue: %+v", proc.Id, msg)
	topic := fmt.Sprintf("%s_proc_%d_in", proc.Pipe.Name, proc.Id)
	proc.Pipe.queue.Publish(proc.Pipe.ctx, topic, msg)
}

func (proc *PipelineProcess) OutQueue(msg PipelineMessage) {
	log.Printf("Process %d: OutQueue: %+v", proc.Id, msg)
	topic := fmt.Sprintf("%s_proc_%d_out", proc.Pipe.Name, proc.Id)
	proc.Pipe.queue.Publish(proc.Pipe.ctx, topic, msg)
}

func NewPipelineProcess(name string, inProcess func(PipelineProcess, PipelineMessage) PipelineMessage, outProcess ...func(PipelineProcess, PipelineMessage) PipelineMessage) (PipelineProcess, error) {
	log.Printf("Creating New Process %s.", name)
	proc := PipelineProcess{
		Name:        name,
		inProcess:   inProcess,
		Concurrency: 1, // support spinning up multiple workers natively
	}
	if len(outProcess) > 0 {
		proc.outProcess = outProcess[0]
	}
	log.Printf("Process %s Created", name)
	return proc, nil
}

type Pipeline struct {
	Name            string
	queue           Queue
	consumerGroup   string
	processes       []*PipelineProcess
	twoWay          bool
	wg              *sync.WaitGroup
	ctx             context.Context
	cancelFunc      context.CancelFunc
	consumerChannel PipelineChannel
}

func (pipe *Pipeline) AddProcess(proc *PipelineProcess) {
	log.Printf("Pipeline: Adding Process %s to Pipeline %s", proc.Name, pipe.Name)
	proc.Id = len(pipe.processes)
	proc.Pipe = pipe
	pipe.processes = append(pipe.processes, proc)
}

func NewPipeline(name string, twoWay bool, wg *sync.WaitGroup, queue Queue, consumerChannel ...PipelineChannel) (Pipeline, error) {
	log.Printf("Creating New Pipeline %s with twoWay as %t", name, twoWay)
	var temp []*PipelineProcess
	ctx, cancel := context.WithCancel(context.Background())
	pipe := Pipeline{
		Name:          name,
		queue:         queue,
		consumerGroup: name + "_group",
		processes:     temp,
		twoWay:        twoWay,
		wg:            wg,
		ctx:           ctx,
		cancelFunc:    cancel,
	}
	if len(consumerChannel) == 1 {
		pipe.consumerChannel = consumerChannel[0]
	} else if len(consumerChannel) > 1 {
		return pipe, errors.New("Only one consumerChannel can be passed as argument")
	}
	return pipe, nil
}

func (pipe *Pipeline) SendMessage(msg PipelineMessage) {
	msg.LastProcess = -1
	topic := fmt.Sprintf("%s_orchestrator", pipe.Name)
	pipe.queue.Publish(pipe.ctx, topic, msg)
}

func (pipe *Pipeline) Start() {
	pipe.wg.Add(1)
	log.Printf("Pipeline: Starting Pipeline %s", pipe.Name)
	for _, proc := range pipe.processes {
		for i := 0; i < proc.Concurrency; i++ {
			log.Printf("Pipeline: Starting Process %s with id %d concurrency worker %d", proc.Name, proc.Id, i)
			go proc.InProcess(pipe.ctx, pipe.queue)
			if pipe.twoWay {
				go proc.OutProcess(pipe.ctx, pipe.queue)
			}
		}
	}
	if pipe.twoWay {
		pipe.twoWayLoop()
	} else {
		pipe.oneWayLoop()
	}
}

func (pipe *Pipeline) Stop() {
	pipe.cancelFunc()
	pipe.wg.Done()
}

func (pipe *Pipeline) oneWayLoop() {
	log.Printf("Pipeline %s: Staring OneWay Loop", pipe.Name)
	topic := fmt.Sprintf("%s_orchestrator", pipe.Name)
	ch, err := pipe.queue.Subscribe(pipe.ctx, topic, pipe.consumerGroup)
	if err != nil {
		return
	}

	for {
		select {
		case <-pipe.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			log.Printf("Pipeline %s: Received msg %+v", pipe.Name, msg)
			if msg.Finished {
				continue
			}
			if msg.LastProcess < len(pipe.processes)-1 {
				nextTopic := fmt.Sprintf("%s_proc_%d_in", pipe.Name, msg.LastProcess+1)
				pipe.queue.Publish(pipe.ctx, nextTopic, msg)
			} else {
				if pipe.consumerChannel != nil {
					log.Printf("Pipeline %s: sending msg to consumer", pipe.Name)
					pipe.consumerChannel <- msg
				}
			}
		}
	}
}

func (pipe *Pipeline) twoWayLoop() {
	log.Printf("Pipeline %s: Staring TwoWay Loop", pipe.Name)
	topic := fmt.Sprintf("%s_orchestrator", pipe.Name)
	ch, err := pipe.queue.Subscribe(pipe.ctx, topic, pipe.consumerGroup)
	if err != nil {
		return
	}
	for {
		select {
		case <-pipe.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			log.Printf("Pipeline %s: Received msg %+v", pipe.Name, msg)
			if msg.Direction == PipelineOutDirection {
				log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
				if msg.LastProcess > 0 {
					nextTopic := fmt.Sprintf("%s_proc_%d_out", pipe.Name, msg.LastProcess-1)
					pipe.queue.Publish(pipe.ctx, nextTopic, msg)
					continue
				} else {
					if pipe.consumerChannel != nil {
						log.Printf("Pipeline %s: sending msg to consumer", pipe.Name)
						pipe.consumerChannel <- msg
					}
				}
			} else if msg.Direction == PipelineInDirection {
				if msg.Finished {
					if msg.LastProcess > 0 {
						log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
						msg.Direction = PipelineOutDirection
						nextTopic := fmt.Sprintf("%s_proc_%d_out", pipe.Name, msg.LastProcess)
						pipe.queue.Publish(pipe.ctx, nextTopic, msg)
						continue
					}
				}
				if msg.LastProcess < len(pipe.processes)-1 {
					log.Printf("Pipeline %s: Passing to in proc", pipe.Name)
					nextTopic := fmt.Sprintf("%s_proc_%d_in", pipe.Name, msg.LastProcess+1)
					pipe.queue.Publish(pipe.ctx, nextTopic, msg)
					continue
				} else if msg.LastProcess == len(pipe.processes)-1 {
					log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
					msg.Finished = true
					msg.Direction = PipelineOutDirection
					nextTopic := fmt.Sprintf("%s_proc_%d_out", pipe.Name, msg.LastProcess)
					pipe.queue.Publish(pipe.ctx, nextTopic, msg)
					continue
				}
			}
		}
	}
}
