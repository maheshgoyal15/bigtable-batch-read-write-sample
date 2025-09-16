# Bigtable Batch Row Reader

A Java application for reading a fixed list of rows from Google Cloud Bigtable in configurable batches. The tool supports both sequential and parallel processing modes, providing detailed performance metrics including total execution time and cell count statistics.


## Installation

### From source

1. Clone the repository:
```bash
git clone https://github.com/maheshgoyal15/bigtable-batch-read-write-sample
cd bigtable-batch-read-write-sample
```

2. Build the application jar:
```bash
mvn clean package
```
## Usage

### Basic Usage

```bash
java -cp target/bigtable-reader-1.0-SNAPSHOT.jar com.example.BigtableBatchRowReader <project_id> <instance_id> <table> <batch_size> <use_threads> [thread_count]
```

### Examples

#### Sequential Processing
Read 100 rows in batches of 20, processing sequentially:
```bash
java -cp target/bigtable-reader-1.0-SNAPSHOT.jar com.example.BigtableBatchRowReader my-project my-instance my-table 20 false
```

#### Parallel Processing
Read 100 rows in batches of 10, using 8 parallel threads:
```bash
java -jar bigtable-batch-reader.jar my-project my-instance my-table 10 true 8
```

#### Small Batch Processing
Read 100 rows in batches of 5, using default 4 threads:
```bash
java -jar bigtable-batch-reader.jar my-project my-instance my-table 5 true
```

## Sample Output

```
Starting Bigtable Batch Row Read:
  Project ID: my-project
  Table Name: my-table
  Total Rows to Read: 100
  Batch Size: 20
  Mode: Parallel (Threads: 4)
----------------------------------------
Parallel: Starting batch 1 of 5 (Keys count: 20) in thread pool-1-thread-1
Parallel: Starting batch 2 of 5 (Keys count: 20) in thread pool-1-thread-2
...

==================================================
**Read Complete!**
Total Rows Read: **100/100**
Total Cells Read: **1,250**
Overall Time: **2,340 ms**
==================================================
```

## Configuration

### Row Keys
The application reads from a predefined list of 100 row keys stored in the `RowKeyData.ALL_ROWKEYS` constant. Modify this list in the `RowKeyData` class to customize which rows are read.

### Performance Tuning
- **Batch Size**: Larger batches reduce API calls but increase memory usage
- **Thread Count**: More threads can improve parallelism but may hit Bigtable rate limits
- **Sequential vs Parallel**: Use sequential for debugging, parallel for performance

### Google Cloud Authentication
Ensure your environment has proper authentication configured:
```bash
# Using service account key
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"

# Or using gcloud CLI
gcloud auth application-default login
```


## Contributing

Contributions are welcome! Please ensure your code follows the existing style and includes appropriate error handling and logging.

## License

This project is released under the Apache 2.0 License.

---

For questions or support, please refer to the [Google Cloud Bigtable documentation](https://cloud.google.com/bigtable/docs).