package io.storage;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.util.Scanner;
public class StorageApplication{

    public static void main(String[] args) throws Exception {
        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB rocksDB = RocksDB.open(options, "rocksdb_data");

        // Read data from JSON file
        TestDataImporter.importData(rocksDB, "/Users/fxf/otherProjects/tools/storage/core/src/main/resources/test.json");

        // Query system
        Scanner scanner = new Scanner(System.in);
        QueryProcessor queryProcessor = new QueryProcessor(rocksDB);

        // Interactive console loop
        while (true) {
            System.out.print("Enter query (or 'exit' to quit): ");
            String query = scanner.nextLine();

            if (query.equalsIgnoreCase("exit")) {
                break;
            }

            queryProcessor.processQuery(query);
        }

        // Close resources
        rocksDB.close();
        options.close();
    }
}
