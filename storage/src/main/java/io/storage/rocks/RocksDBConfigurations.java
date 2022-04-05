package io.storage.rocks;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Configuration
@Slf4j
public class RocksDBConfigurations {

    private final static String FILE_NAME = "storage-db";

    @Bean(destroyMethod = "close")
    public RocksDB rocksDB() {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        File baseDir = new File("/tmp/rocks", FILE_NAME);

        try {
            Files.createDirectories(baseDir.getParentFile().toPath());
            Files.createDirectories(baseDir.getAbsoluteFile().toPath());

            final RocksDB db = RocksDB.open(options, baseDir.getAbsolutePath());
            log.info("RocksDB initialized");
            return db;
        } catch (IOException | RocksDBException e) {
            log.error("Error initializng RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage(), e);
            throw new AssertionError(e);
        }
    }
}
