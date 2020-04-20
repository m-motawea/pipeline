package pipeline

import (
	"errors"
	"log"
	"sync"
)

type PipelineDirection interface{}
type PipelineInDirection struct{}
type PipelineOutDirection struct{}

type PipelineMessage struct {
	LastProcess int
	Direction   PipelineDirection
	Content     interface{}
	Finished    bool
	Drop        bool
}

type PipelineChannel chan PipelineMessage

type PipelineProcess struct {
	Id           int
	Name         string
	inProcess    func(PipelineProcess, PipelineMessage) PipelineMessage
	outProcess   func(PipelineProcess, PipelineMessage) PipelineMessage
	inChannel    PipelineChannel
	outChannel   PipelineChannel
	CloseChannel chan int
	Pipe         *Pipeline
}

func (proc *PipelineProcess) InProcess(pipeChannel PipelineChannel) {
	log.Printf("Process: Staring InProcess for %s id: %d", proc.Name, proc.Id)
	for {
		select {
		case <-proc.CloseChannel:
			log.Println("InProcess: Closing.")
			return
		case msg := <-proc.inChannel:
			log.Printf("Process %s: InProcess: Received Message %+v", proc.Name, msg)
			res := proc.inProcess(*proc, msg)
			if msg.Drop {
				continue
			}
			res.LastProcess = proc.Id
			log.Printf("Process %s: InProcess: inFunc Result %+v", proc.Name, res)
			pipeChannel <- res
		}
	}
}

func (proc *PipelineProcess) OutProcess(pipeChannel PipelineChannel) {
	log.Printf("Process: Staring OutProcess for %s id: %d", proc.Name, proc.Id)
	for {
		select {
		case <-proc.CloseChannel:
			log.Printf("Process %s: OutProcess: Closing.", proc.Name)
			return
		case msg := <-proc.outChannel:
			log.Printf("Process %s: OutProcess: Received Message %+v", proc.Name, msg)
			res := proc.outProcess(*proc, msg)
			if msg.Drop {
				continue
			}
			res.LastProcess = proc.Id
			log.Printf("Process %s: OutProcess: inFunc Result %+v", proc.Name, res)
			pipeChannel <- res
		}
	}
}

func (proc *PipelineProcess) Close() {
	proc.CloseChannel <- 1
}

func (proc *PipelineProcess) InQueue(msg PipelineMessage) {
	log.Printf("Process %d: InQueue: %+v", proc.Id, msg)
	proc.inChannel <- msg
}

func (proc *PipelineProcess) OutQueue(msg PipelineMessage) {
	log.Printf("Process %d: OutQueue: %+v", proc.Id, msg)
	proc.outChannel <- msg
}

func NewPipelineProcess(name string, inProcess func(PipelineProcess, PipelineMessage) PipelineMessage, outProcess ...func(PipelineProcess, PipelineMessage) PipelineMessage) (PipelineProcess, error) {
	log.Printf("Creating New Process %s.", name)
	proc := PipelineProcess{
		Name:         name,
		inProcess:    inProcess,
		inChannel:    make(PipelineChannel),
		CloseChannel: make(chan int),
	}
	if len(outProcess) > 0 {
		proc.outProcess = outProcess[0]
		proc.outChannel = make(PipelineChannel)
	}
	log.Printf("Process %s Created", name)
	return proc, nil
}

type Pipeline struct {
	Name            string
	pipeChannel     PipelineChannel
	processes       []*PipelineProcess
	twoWay          bool
	closeChannel    chan int
	wg              *sync.WaitGroup
	consumerChannel PipelineChannel
}

func (pipe *Pipeline) AddProcess(proc *PipelineProcess) {
	log.Printf("Pipeline: Adding Process %s to Pipeline %s", proc.Name, pipe.Name)
	proc.Id = len(pipe.processes)
	proc.Pipe = pipe
	pipe.processes = append(pipe.processes, proc)
}

func NewPipeline(name string, twoWay bool, wg *sync.WaitGroup, consumerChannel ...PipelineChannel) (Pipeline, error) {
	log.Printf("Creating New Pipeline %s with twoWay as %t", name, twoWay)
	var temp []*PipelineProcess
	pipe := Pipeline{
		Name:         name,
		pipeChannel:  make(PipelineChannel),
		processes:    temp,
		twoWay:       twoWay,
		closeChannel: make(chan int),
		wg:           wg,
	}
	if len(consumerChannel) == 1 {
		pipe.consumerChannel = consumerChannel[0]
	} else {
		return pipe, errors.New("Only one consumerChannel can be passed as argument")
	}
	return pipe, nil
}

func (pipe *Pipeline) SendMessage(msg PipelineMessage) {
	msg.LastProcess = -1
	pipe.pipeChannel <- msg
}

func (pipe *Pipeline) Start() {
	pipe.wg.Add(1)
	log.Printf("Pipeline: Starting Pipeline %s", pipe.Name)
	for _, proc := range pipe.processes {
		log.Printf("Pipeline: Starting Process %s with id %d", proc.Name, proc.Id)
		go proc.InProcess(pipe.pipeChannel)
		if pipe.twoWay {
			go proc.OutProcess(pipe.pipeChannel)
		}
	}
	if pipe.twoWay {
		pipe.twoWayLoop()
	} else {
		pipe.oneWayLoop()
	}
}

func (pipe *Pipeline) Stop() {
	pipe.closeChannel <- 1
	pipe.wg.Done()
}

func (pipe *Pipeline) oneWayLoop() {
	log.Printf("Pipeline %s: Staring OneWay Loop", pipe.Name)
	for {
		select {
		case <-pipe.closeChannel:
			for _, proc := range pipe.processes {
				proc.Close()
				return
			}
		case msg := <-pipe.pipeChannel:
			log.Printf("Pipeline %s: Received msg %+v", pipe.Name, msg)
			if msg.Finished {
				continue
			}
			if msg.LastProcess < len(pipe.processes)-1 {
				nextProc := pipe.processes[msg.LastProcess+1]
				go nextProc.InQueue(msg)
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
	for {
		select {
		case <-pipe.closeChannel:
			for _, proc := range pipe.processes {
				proc.Close()
				return
			}
		case msg := <-pipe.pipeChannel:
			log.Printf("Pipeline %s: Received msg %+v", pipe.Name, msg)
			/*
				1- Check direction:
					- If Out:
						- Check LastProcess
							- If 0: Drop
							- If > 0: Pass to LastProcess - 1 Out
					- If In:
						- Check Finished:
							- If true: Pass to LastProcess - 1 Out
							- If false: Check LastProcess:
								- If < len(pipe.Processes) - 1: Pass to LastProcess + 1 In
								- Else: set Finished = True, Pass to LastProcess Out
			*/
			if (msg.Direction == PipelineOutDirection{}) {
				log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
				if msg.LastProcess > 0 {
					nexProc := pipe.processes[msg.LastProcess-1]
					go nexProc.OutQueue(msg)
					continue
				} else {
					if pipe.consumerChannel != nil {
						log.Printf("Pipeline %s: sending msg to consumer", pipe.Name)
						pipe.consumerChannel <- msg
					}
				}
			} else if (msg.Direction == PipelineInDirection{}) {
				if msg.Finished {
					if msg.LastProcess > 0 {
						log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
						nexProc := pipe.processes[msg.LastProcess-1]
						msg.Direction = PipelineOutDirection{}
						go nexProc.OutQueue(msg)
						continue
					}
				}
				if msg.LastProcess < len(pipe.processes)-1 {
					log.Printf("Pipeline %s: Passing to in proc", pipe.Name)
					nextProc := pipe.processes[msg.LastProcess+1]
					go nextProc.InQueue(msg)
					continue
				} else if msg.LastProcess == len(pipe.processes)-1 {
					log.Printf("Pipeline %s: Passing to out proc", pipe.Name)
					msg.Finished = true
					msg.Direction = PipelineOutDirection{}
					nextProc := pipe.processes[msg.LastProcess]
					go nextProc.OutQueue(msg)
					continue
				}
			}
		}
	}
}
