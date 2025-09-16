package com.example;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId; // Import TableId

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Java application to read a fixed list of 100 Bigtable rows in configurable batches.
 * Supports reading batches sequentially or in parallel using a thread pool.
 * It tracks the total time taken and the total number of cells read.
 */
public class BigtableBatchRowReader {

    private static final int TOTAL_ROW_KEYS = 100;
    private final AtomicLong totalCellsRead = new AtomicLong(0);

    public static void main(String[] args) {
        // 1. Check for required command-line arguments (at least 5)
        if (args.length < 5) {
            System.err.println("Usage: java -jar <jar>.jar <project_id> <instance_id> <table> <batch_size> <use_threads> [thread_count]");
            System.err.println("  <use_threads> should be 'true' or 'false'.");
            System.exit(1);
        }

        // 2. Parse arguments
        String projectId = args[0];
        String instanceId = args[1];
        String tableName = args[2];
        int batchSize = Integer.parseInt(args[3]);
        boolean useThreadPool = Boolean.parseBoolean(args[4]);
        int threadCount = useThreadPool && args.length > 5 ? Integer.parseInt(args[5]) : 4;

        BigtableBatchRowReader reader = new BigtableBatchRowReader();
        List<String> allRowKeys = RowKeyData.ALL_ROWKEYS;

        // Create the TableId object once
        TableId tableId = TableId.of(tableName);

        // 3. Print configuration
        System.out.println("Starting Bigtable Batch Row Read:");
        System.out.println("  Project ID: " + projectId);
        System.out.println("  Table Name: " + tableName);
        System.out.println("  Total Rows to Read: " + TOTAL_ROW_KEYS);
        System.out.println("  Batch Size: " + batchSize);
        System.out.println("  Mode: " + (useThreadPool ? "Parallel (Threads: " + threadCount + ")" : "Sequential"));
        System.out.println("----------------------------------------");

        List<String> readRowKeys = new ArrayList<>();
        Instant start = Instant.now();

        try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {

            if (useThreadPool) {
                // Pass the TableId object
                readRowKeys = reader.readRowsParallel(dataClient, tableId, allRowKeys, batchSize, threadCount);
            } else {
                // Pass the TableId object
                readRowKeys = reader.readRowsSequential(dataClient, tableId, allRowKeys, batchSize);
            }

        } catch (IOException e) {
            System.err.println(" Failed to create BigtableDataClient: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println(" An error occurred during row reading: " + e.getMessage());
            e.printStackTrace();
        }

        Instant end = Instant.now();

        // 4. Print overall results
        long totalTimeMs = Duration.between(start, end).toMillis();

        System.out.println("\n" + "=".repeat(50));
        System.out.println("✅ **Read Complete!**");
        System.out.println("Total Rows Read: **" + readRowKeys.size() + "/" + TOTAL_ROW_KEYS + "**");
        System.out.println("Total Cells Read: **" + reader.totalCellsRead.get() + "**");
        System.out.println("Overall Time: **" + totalTimeMs + " ms**");
        System.out.println("=".repeat(50));

        // 5. Print only the row keys
        System.out.println("Successfully Read Row Keys (" + readRowKeys.size() + "):\n" + readRowKeys);
    }

    /**
     * Reads rows in batches sequentially in the main thread.
     */
    public List<String> readRowsSequential(BigtableDataClient dataClient, TableId tableId,
                                           List<String> allRowKeys, int batchSize) {
        List<String> readRowKeys = new ArrayList<>();
        int totalRows = allRowKeys.size();
        int batchNumber = 1;
        int totalBatches = (int) Math.ceil((double) totalRows / batchSize);

        for (int i = 0; i < totalRows; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalRows);
            List<String> batchKeys = allRowKeys.subList(i, endIndex);

            System.out.printf("Sequential: Reading batch %d of %d (Keys count: %d)\n",
                    batchNumber++, totalBatches, batchKeys.size());

            readRowKeys.addAll(readBatch(dataClient, tableId, batchKeys));
        }
        return readRowKeys;
    }

    /**
     * Reads rows in batches in parallel using a fixed-size thread pool.
     */
    public List<String> readRowsParallel(BigtableDataClient dataClient, TableId tableId,
                                         List<String> allRowKeys, int batchSize, int threadCount)
            throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<String> readRowKeys = new ArrayList<>();
        int totalRows = allRowKeys.size();

        // 1. Split all keys into batches
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < totalRows; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalRows);
            batches.add(allRowKeys.subList(i, endIndex));
        }

        // 2. Submit each batch read as a task to the thread pool
        List<Future<List<String>>> futures = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            final int batchIndex = i + 1;
            List<String> batch = batches.get(i);
            Future<List<String>> future = executor.submit(() -> {
                System.out.printf("Parallel: Starting batch %d of %d (Keys count: %d) in thread %s\n",
                        batchIndex, batches.size(), batch.size(), Thread.currentThread().getName());
                return readBatch(dataClient, tableId, batch);
            });
            futures.add(future);
        }

        // 3. Wait for all tasks to complete and collect results
        for (Future<List<String>> future : futures) {
            try {
                readRowKeys.addAll(future.get());
            } catch (ExecutionException e) {
                System.err.println("Error reading a batch: " + e.getCause().getMessage());
            }
        }

        // 4. Shutdown the executor
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            System.err.println("Thread pool did not terminate gracefully within 60 seconds.");
        }

        return readRowKeys;
    }

    /**
     * The core logic for reading a batch of keys and counting cells.
     * FIX: Uses TableId directly in Query.create() to match the sample.
     */
    private List<String> readBatch(BigtableDataClient dataClient, TableId tableId, List<String> rowKeys) {
        List<String> readKeys = new ArrayList<>();

        // 1. Create a Query object using the public Query.create(TableId) overload
        Query query = Query.create(tableId);

        // 2. Add all row keys from the batch to the query
        for (String key : rowKeys) {
            query = query.rowKey(key);
        }

        // 3. Execute the multi-row query and iterate over the resulting rows
        for (Row row : dataClient.readRows(query)) {
            // a. Read each and every cell and record number of cells.
            //totalCellsRead.addAndGet(row.getCells().size());

            // b. Record the row key of the successfully read row.
            readKeys.add(row.getKey().toStringUtf8());
        }

        return readKeys;
    }
}