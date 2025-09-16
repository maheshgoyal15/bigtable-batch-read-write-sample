package com.example;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import java.io.IOException;
import java.util.Optional;

/**
 * A Java application that calculates the total size of a single row in a Bigtable table.
 * It retrieves the specified row and sums the sizes of the row key, column family,
 * column qualifier, and value for each cell.
 *
 * To run, compile with the Google Cloud Bigtable dependency and execute with:
 *
 * java -jar <your_jar_file>.jar <project_id> <instance_id> <table> <row_key>
 */
public class BigtableGetRowSize {

    public static void main(String[] args) {
        // Check for required command-line arguments
        if (args.length < 4) {
            System.err.println("Usage: java -jar <your_jar_file>.jar <project_id> <instance_id> <table> <row_key>");
            System.exit(1);
        }

        String projectId = args[0];
        String instanceId = args[1];
        String tableName = args[2];
        String rowKey = args[3];

        System.out.println("Starting row size calculation for Bigtable:");
        System.out.println("  Project ID: " + projectId);
        System.out.println("  Instance ID: " + instanceId);
        System.out.println("  Table Name: " + tableName);
        System.out.println("  Row Key: " + rowKey);
        System.out.println("----------------------------------------");

        try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {

            // The readRow method requires the TableId and the rowKey as separate arguments.
            Row optionalRow = dataClient.readRow(TableId.of(tableName), rowKey);


                long totalSize = 0;

                // Iterate through all cells in the row and sum their sizes.
                for (RowCell cell : optionalRow.getCells()) {
                    totalSize += optionalRow.getKey().size();
                    totalSize += cell.getFamily().length();
                    totalSize += cell.getQualifier().size();
                    totalSize += cell.getValue().size();
                }

                System.out.println("----------------------------------------");
                System.out.printf("Total size of row '%s' is %d bytes%n", rowKey, totalSize);


        } catch (IOException e) {
            System.err.println("Failed to create BigtableDataClient: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
