### Basic Structures:
##### 1- pipeline.PipelineMessage:
```go
type PipelineMessage struct {
	LastProcess int
	Direction   PipelineDirection
	Content     interface{}
	Finished    bool
	Drop        bool
}
```
It is the data format which is passed through the routines of the pipeline. You can store your data inside its ```Content``` member.
A message enters the pipeline using ```func (pipe *Pipeline) SendMessage(msg PipelineMessage)```.


##### 2- pipeline.Pipeline:
```go
type Pipeline struct {
	Name            string
	pipeChannel     PipelineChannel
	processes       []*PipelineProcess
	twoWay          bool
	closeChannel    chan int
	wg              *sync.WaitGroup
	consumerChannel PipelineChannel
}
```
A pipeline is composed of multiple processes executing on a message in order until:
1- Message is set as Finished by a process.
2- Message is set as Drop by a process.
3- No more processes in the pipeline.

A pipeline can pass messages to processes either in one way (up the pipeline) in which each process operates one time at max on the message, or in two way (Up then down the pipeline) in which each process operates two times at max on the message.

To create a pipeline you can use ```func NewPipeline(name string, twoWay bool, wg *sync.WaitGroup, consumerChannel ...PipelineChannel) (Pipeline, error)```:
```go
// Create a one way pipeline
var wg sync.WaitGroup
pipe1, _ := NewPipeline("test_pipeline", false, &wg)

// Create a two way pipeline
var wg sync.WaitGroup
pipe2, _ := NewPipeline("test_pipeline", true, &wg)
```

You can pass an optional parameter of type ```pipeline.PipelineChannel``` when creating a pipeline which is used to send the result of the pipeline.


##### 3- pipeline.PipelineProcess:
```go
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
```

A pipeline process represents a stage of the pipeline.
In a one way pipeline, a process needs to have only ```inProcess``` member set.
In a two way pipeline, a process needs to have both ```inProcess``` and ```outProcess``` members set.

```inProcess``` operates on the message as it traverses the pipeline before it changes direction in which case ```outProcess``` will be called.

To create a process you can use ```func NewPipelineProcess(name string, inProcess func(PipelineProcess, PipelineMessage) PipelineMessage, outProcess ...func(PipelineProcess, PipelineMessage) PipelineMessage) (PipelineProcess, error)```:
```go
Func := func(proc PipelineProcess, msg PipelineMessage) PipelineMessage {
		log.Println("inFunc received msg")
		val, ok := msg.Content.(int)
		if ok {
			
			msg.Content = val + 1
		}
		return msg
	}

// Create a process used for a one way pipeline
proc1, _ := NewPipelineProcess("test_proc1", Func)
// Create a process used for a two way pipeline
proc2, _ := NewPipelineProcess("test_proc2", Func, Func)
```

To add a process to a pipeline use ```func (pipe *Pipeline) AddProcess(proc *PipelineProcess)```:

