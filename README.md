![Go](https://github.com/m-motawea/pipeline/workflows/Go/badge.svg?branch=master)

An advanced framework for pipeline processing built to process messages in a structured, staged manner similar to network switches and routers. This project natively supports **horizontal scaling** and **distributed workloads** via pluggable message queue drivers (Local Channels, Redis, AMQP/RabbitMQ). This permits launching thousands of goroutines within one process, or physically distributing nodes on servers to load balance pipeline stages.

### Basic Structures & Usage:

##### 1- pipeline.PipelineMessage:
```go
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
```
The standard object passed through the pipeline stages. Since pipeline components routinely span over the network, `Content` explicitly accepts binary byte slices (`[]byte`). Clients are expected to easily parse (`json.Marshal`) structures internally within process logic stages.
A message enters the initial queue via `func (pipe *Pipeline) SendMessage(msg PipelineMessage)`.

##### 2- pipeline.Queue Drivers:
The pipeline internally routes messages between discrete processes exclusively by utilizing dependencies fulfilling the `pipeline.Queue` interface. First-party implementations include:

- **LocalQueue (`github.com/m-motawea/pipeline/localqueue`)**: Leverages standard Go buffered channels for extremely fast isolated concurrent processing locally.
- **RedisQueue (`github.com/m-motawea/pipeline/redisqueue`)**: Leverages Redis Lists natively relying on `BLPOP` to structurally enforce single-delivery worker lock load distribution across multiple servers.
- **AMQPQueue (`github.com/m-motawea/pipeline/amqpqueue`)**: Connects to RabbitMQ architectures natively mapping active consumer channels.

##### 3- pipeline.Pipeline:
A pipeline is composed of multiple processes executing sequentially on a message in order until:
1. Message is set as `Finished` by a process.
2. Message is set as `Drop` by a process.
3. No more processes exist linearly in the pipeline.

Messages traverse either *one-way* (up the pipeline routing) or *two-ways* (up then down reflecting backwards dynamically).

To initialize a pipeline by binding your configured queue driver:
```go
import "github.com/m-motawea/pipeline/localqueue"

var wg sync.WaitGroup
driver := localqueue.NewLocalQueue()

// Create a one way pipeline
pipe1, _ := pipeline.NewPipeline("production_pipeline", false, &wg, driver)
```

##### 4- pipeline.PipelineProcess:
```go
type PipelineProcess struct {
	Id           int
	Name         string
	Concurrency  int
	// ... unexported parameters
}
```

A pipeline process represents a configured stage of the pipeline graph. `Concurrency` explicitly dictates the amount of discrete workers/goroutines to spontaneously spawn when evaluating messages on the queue asynchronously.

To construct a process, define your manipulation tracking evaluating `msg.Content` bytes:
```go
inFunc := func(proc pipeline.PipelineProcess, msg pipeline.PipelineMessage) pipeline.PipelineMessage {
    // Process input data logic 
    log.Printf("Worker processing on stage %s", proc.Name)
    // Marshal customized updates...
    return msg
}

// Instantiate the distinct processing node
proc1, _ := pipeline.NewPipelineProcess("stage_1_authentication", inFunc)

// Scale concurrency to 10 lightweight thread workers instantly bridging the local queue!
proc1.Concurrency = 10 

pipe1.AddProcess(&proc1)
pipe1.Start() // Initiates standard asynchronous worker polls.
```

## Running Distributed Tests
To see real functional distributed deployment examples running separate CLI apps communicating reliably through Redis or RabbitMQ architectures, view the `examples/distributed` directory!
