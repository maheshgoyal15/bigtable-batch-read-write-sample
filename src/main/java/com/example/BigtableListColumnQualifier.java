package com.example;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.TableId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A simple Java application to read data from Google Cloud Bigtable using command-line arguments.
 * It uses the Google Cloud Bigtable Data Client library.
 *
 * This version of the application does not use any external dependencies for
 * command-line argument parsing. It expects the arguments to be provided in a specific order.
 *
 * To compile and run, you will need to include the following dependency in your project
 * (e.g., in a Maven pom.xml file):
 *
 * <dependency>
 * <groupId>com.google.cloud</groupId>
 * <artifactId>google-cloud-bigtable</artifactId>
 * <version>2.29.0</version>
 * </dependency>
 */
public class BigtableListColumnQualifier {

    public static void main(String[] args) {
        // Check for required command-line arguments.
        if (args.length < 4) {
            System.err.println("Usage: java -jar <your_jar_file>.jar <project_id> <instance_id> <table> <column_family> <regex_pattern> [rowKeyPrefix]");
            System.exit(1);
        }

        String projectId = args[0];
        String instanceId = args[1];
        String tableName = args[2];
        String columnFamily = args[3];


        String regexPattern = (args.length > 4) ? args[4] : ".*";
        String rowKeyPrefix = (args.length > 5) ? args[5] : null;

        System.out.println("Reading from Bigtable with the following parameters:");
        System.out.println("  Project ID: " + projectId);
        System.out.println("  Instance ID: " + instanceId);
        System.out.println("  Table Name: " + tableName);
        System.out.println("  Column Family: " + columnFamily);
        System.out.println("  Regex Pattern: " + regexPattern);
        if (rowKeyPrefix != null) {
            System.out.println("  Row Key Prefix: " + rowKeyPrefix);
        }
        System.out.println("----------------------------------------");

        // --- Bigtable Client and Read Operation ---
        try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {

            Query query = Query.create(TableId.of(tableName));

            // Apply filters to match the specified row key, column family, and regex pattern.
            if (rowKeyPrefix != null && !rowKeyPrefix.isEmpty()) {
               // query = query.filter(Filters.FILTERS.rowKey().exactMatch(rowKeyPrefix));
            }

            query = query.filter(Filters.FILTERS.chain()
                    .filter(Filters.FILTERS.family().exactMatch(columnFamily))
                    .filter(Filters.FILTERS.qualifier().regex(regexPattern)));

            ServerStream<Row> rows = dataClient.readRows(query);

            System.out.println("Successfully connected to Bigtable. Reading rows...");

            for (Row row : rows) {
                printQualifiers(row);
            }

            System.out.println("----------------------------------------");
            System.out.println("Read operation complete.");

        } catch (IOException e) {
            System.err.println("Failed to create BigtableDataClient: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Helper method to print the row key and all column qualifiers of a Bigtable row.
     * It uses a Set to ensure each qualifier is printed only once per row.
     *
     * @param row The Bigtable Row object to print.
     */
    private static void printQualifiers(Row row) {
        System.out.println("Row Key: " + row.getKey().toStringUtf8());
        Set<String> uniqueQualifiers = new HashSet<>();
        row.getCells().forEach(cell -> {
            uniqueQualifiers.add(cell.getQualifier().toStringUtf8());
        });

        System.out.println("  Qualifiers:");
        uniqueQualifiers.forEach(qualifier -> {
            System.out.println("    - " + qualifier);
        });

        System.out.println(); // Add a blank line for readability
    }
}
