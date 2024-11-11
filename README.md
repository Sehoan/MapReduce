# MapReduce

[MapReduce](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf) is a programming model and processing technique for distributed computing that processes and generates large datasets across parallel, distributed clusters. Originally developed by Google, it simplifies data processing on large clusters.

## How it Works

The programming model consists of two primary operations:

- **Map**: 
  - Takes input data and splits it into smaller chunks
  - Processes each chunk to generate intermediate key-value pairs
  - Example: `map(key string, value string) -> []KeyValue`

- **Reduce**:
  - Accepts the intermediate key-value pairs from Map phase
  - Merges values associated with the same key
  - Produces the final output
  - Example: `reduce(key string, values []string) -> string`

## Key Features

- **Parallel Processing**: Automatically parallelizes tasks across multiple machines
- **Fault Tolerance**: 
  - Handles machine failures gracefully
  - Automatically re-executes failed tasks
- **Scalability**: 
  - Scales horizontally by adding more machines to the cluster
  - Processes petabytes of data across thousands of machines
- **Data Locality**: Moves computation to data instead of moving data to computation
- **Simple Programming Model**: Abstracts away the complexity of distributed computing

## Common Use Cases

- Log Analysis and Processing
- Search Engine Indexing
- Machine Learning and Data Mining
- ETL (Extract, Transform, Load) Operations
- Graph Processing
- Scientific Data Processing

## Implementation Examples

Popular implementations include:
- Apache Hadoop MapReduce
- Apache Spark (extends the model with more operations)
- Google Cloud Dataflow
- Amazon EMR (Elastic MapReduce)

## Getting Started

To implement a basic MapReduce job:
1. Create a new go file, `src/mrapps/new_job.go` similar to `src/mrapps/wc.go`
2. Define your Map function
3. Define your Reduce function
4. Prepare your input data
5. Configure your cluster
6. Run `go build -buildmode=plugin src/mrapps/new_job.go` to build the new job as a plugin
7. Run `go run src/main/mrcoordinator.go [input files]` in the cluster running a coordinator
8. Run `go run src/main/mrworker.go src/mrapps/new_job.so` in the clusters running workers
9. Submit and monitor your job

> See `src/main/test-mr.sh` for reference

## Best Practices

- Design your keys carefully for optimal partitioning
- Implement combiners when possible to optimize network usage
- Use appropriate data types and serialization methods
- Consider memory constraints in your implementation
- Test with small datasets before scaling up

## Performance Considerations

- Input data size and format
- Number of mappers and reducers
- Network bandwidth between nodes
- Disk I/O performance
- Memory allocation for tasks
