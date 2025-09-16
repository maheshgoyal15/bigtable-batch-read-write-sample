package com.example;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * A Java application that writes a large number of rows and columns to a Bigtable table.
 * It uses the Batcher API for efficient, batched writes. This is the recommended approach for
 * high-volume data ingestion.
 *
 * This application is designed to write a user-specified number of rows and columns.
 * The value for each cell is a dynamically generated string of a random length between 2 KB and 5 KB.
 *
 * To run, compile with the Google Cloud Bigtable dependency and execute with:
 *
 * java -jar <your_jar_file>.jar <project_id> <instance_id> <table> <column_family> <num_rows> <num_columns>
 */
public class BigtableWriteData {

    public static void main(String[] args) {
        // Check for required command-line arguments
        if (args.length < 6) {
            System.err.println("Usage: java -jar <your_jar_file>.jar <project_id> <instance_id> <table> <column_family> <num_rows> <num_columns>");
            System.exit(1);
        }

        String projectId = args[0];
        String instanceId = args[1];
        String tableName = args[2];
        String columnFamily = args[3];
        int numRowkeys = 0;
        int numColumnsPerRow = 0;

        try {
            numRowkeys = Integer.parseInt(args[4]);
            numColumnsPerRow = Integer.parseInt(args[5]);
        } catch (NumberFormatException e) {
            System.err.println("Error: <num_rows> and <num_columns> must be valid integers.");
            System.exit(1);
        }

        System.out.println("Starting data write operation to Bigtable:");
        System.out.println("  Project ID: " + projectId);
        System.out.println("  Instance ID: " + instanceId);
        System.out.println("  Table Name: " + tableName);
        System.out.println("  Column Family: " + columnFamily);
        System.out.println("  Number of rowkeys to write: " + numRowkeys);
        System.out.println("  Number of columns per row: " + numColumnsPerRow);
        System.out.println("----------------------------------------");

        try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {

            // The Batcher API is the preferred way to do bulk writes.
            // It automatically batches mutations and sends them to Bigtable.
            try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(TableId.of(tableName))) {

                // Outer loop to create the specified number of rowkeys.
                for (int i = 0; i < numRowkeys; i++) {
                    String rowKey = "rowkey-" + UUID.randomUUID().toString();
                    System.out.println("Generating data for rowkey: " + rowKey);

                    // Inner loop to generate and add a large number of columns for the current rowkey.
                    for (int j = 0; j < numColumnsPerRow; j++) {
                        // Dynamically generate a unique column qualifier.
                        String columnQualifier = "column-" + j;

                        // Generate a string of a random length between 2 KB and 5 KB.
                        Random random = new Random();
                        int minBytes = 2 * 100; // 2 KB
                        int maxBytes = 5 * 100; // 5 KB
                        int randomLength = random.nextInt(maxBytes - minBytes + 1) + minBytes;
                        StringBuilder sb = new StringBuilder(randomLength);
                        for (int k = 0; k < randomLength; k++) {
                            sb.append('x');
                        }
                        String value = sb.toString();

                        // Create a RowMutationEntry for each row key and add it to the batcher.
                        // The entry contains the rowkey and the mutations to apply.
                        RowMutationEntry entry = RowMutationEntry.create(rowKey)
                                .setCell(columnFamily, ByteString.copyFromUtf8(columnQualifier), ByteString.copyFromUtf8(value));

                        batcher.add(entry);
                    }
                }

                // Flush any remaining mutations that have not been sent yet.
                // The batcher will automatically flush when it is full.
                batcher.flush();

            } catch (BatchingException batchingException) {
                System.err.println("At least one entry failed to apply. Summary of the errors: \n" + batchingException);
                // You can inspect the individual future objects if you need more detailed error information.
            } catch (InterruptedException e) {
                System.err.println("The write operation was interrupted: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("----------------------------------------");
            System.out.println("Data write operation complete. Total of " +
                    (numRowkeys * numColumnsPerRow) + " cells added to the batcher.");

        } catch (IOException e) {
            System.err.println("Failed to create BigtableDataClient: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
