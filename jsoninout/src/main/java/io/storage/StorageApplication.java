package io.storage;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.file.Files;
import java.util.Scanner;

public class StorageApplication {


    public static void main(String[] args) throws Exception {
        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);
        File baseDir = new File("/tmp/rocks", "rocksdb_data");
        {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());
        }
        RocksDB rocksDB = RocksDB.open(options, baseDir.getAbsolutePath());

        // Read test data from JSON
        final var insertProcessor = new InsertProcessor(rocksDB);
        insertProcessor.insertData("test", testData);

        // Query system
        final Scanner scanner = new Scanner(System.in);
        final var queryProcessor = new QueryProcessor(rocksDB);

        //test on test
        queryProcessor.processQuery("test foo bar");

        // Interactive console loop
        while (true) {
            System.out.println("Enter query (or 'exit' to quit)");
            System.out.println("Query: _find {collection_name} {field_name} {value}");
            System.out.println("Insert: _insert {collection_name} {jsonArray}");
            final String query = scanner.nextLine().trim();

            if (query.equalsIgnoreCase("exit")) {
                break;
            }
            if (query.startsWith("_update")) {
                final String[] split = query.replace("_update", "")
                        .trim()
                        .split(" ", 2);
                insertProcessor.insertData(split[0], split[1]);
            }
            if (query.startsWith("_find")) {
                queryProcessor.processQuery(query.replace("_find", "").trim());
            }
        }

        // Close resources
        rocksDB.close();
        options.close();
    }

    private static final String testData = """
            [
              {
                "foo": "bar",
                "enable": true,
                "index": 42,
                "point": 123.12
              },
              {
                "foo": "bar",
                "ping": "PONG",
                "index": 2,
                "point": 123456789012.12345
              },
              {
                "foo": "bor",
                "ping": "ping",
                "enable": true
              },
              {
                "foo": "bor",
                "ping": "ping",
                "enable": true
              }
            ]
            """;
}
