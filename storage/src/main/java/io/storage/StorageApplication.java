package io.storage;

import io.storage.rocks.KVRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Optional;

@Slf4j
@SpringBootApplication
@AllArgsConstructor
public class StorageApplication implements CommandLineRunner {
    private final KVRepository<String, Object> repository;

    public static void main(String[] args) {
        log.info("Starting the storage");

        SpringApplication.run(StorageApplication.class, args);

        log.info("storage finished");
    }

    @Override
    public void run(String... args) {
        log.info("EXECUTING : command line runner");

        switch (args.length) {
            case 2 -> {
                final String key = args[1];
                switch (args[0]) {
                    case "get" -> {
                        final Optional<Object> valueOpt = repository.find(key);
                        valueOpt.ifPresentOrElse(
                                value -> log.info("\n Get key: '{}'; value: '{}'", key, value),
                                () -> log.info("\n Get key: '{}'; NOT FOUND", key));
                    }
                    case "remove" -> {
                        final boolean deleted = repository.delete(key);
                        log.info("\n Remove key: '{}'; deleted: {}", key, deleted);
                    }
                    default -> unexpectedArgs(args);
                }
            }
            case 3 -> {
                switch (args[0]) {
                    case "set" -> {
                        final String key = args[1];
                        final String value = args[2];
                        final boolean saved = repository.save(key, value);
                        log.info("Set key: '{}'; value: '{}'; saved:{}", key, value, saved);
                    }
                    default -> unexpectedArgs(args);
                }
            }
            default -> unexpectedArgs(args);
        }

    }

    private void unexpectedArgs(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            log.warn("Unexpected count of args");
            log.warn("Args[{}]: {}", i, args[i]);
        }
    }
}
