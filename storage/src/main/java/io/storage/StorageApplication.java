package io.storage;

import io.storage.arrow.t.FilterSingleColumnApplication;
import io.storage.arrow.t.GenerateRandomDataApplication;
import io.storage.arrow.t.Person;
import io.storage.arrow.t.ProcessDataApplication;
import io.storage.rocks.KVRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.SerializationUtils;
import org.springframework.util.StopWatch;

import java.util.Optional;

@Slf4j
@SpringBootApplication
@AllArgsConstructor
public class StorageApplication implements CommandLineRunner {
    private final KVRepository<byte[], byte[]> repository;

    public static void main(String[] args) {
        log.info("Starting the storage");

        SpringApplication.run(StorageApplication.class, args);

        log.info("Storage finished");
    }

    @Override
    public void run(String... args) {
        log.info("EXECUTING : command line runner");

        if (false) {// turn off
            runFromConsole(args);
            return;
        }

        try {
            GenerateRandomDataApplication app = new GenerateRandomDataApplication();

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
//            int numberOfPeople = 10_047_031;
            int numberOfPeople = 42;
            log.info("Generating {} people", numberOfPeople);
            Person[] people = app.randomPeople(numberOfPeople);
            stopWatch.stop();
            stopWatch.start();
            log.info("Initiating writing");


            app.writeToArrowFile(people, repository); //// writeToRocksDb

            stopWatch.stop();
            log.info("GenerateRandomDataApplication Timing: {}", stopWatch);
        } catch (Exception e) {
            log.error("Can't write arrow to rocks", e);
        }
        try {
            FilterSingleColumnApplication app = new FilterSingleColumnApplication();

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();


            app.doAnalytics(repository); //// readFromRocksDb

            stopWatch.stop();
            log.info("FilterSingleColumnApplication Timing: {}", stopWatch);
        } catch (Exception e) {
            log.error("Can't FilterSingleColumnApplication.doAnalytics with arrow from rocks", e);
        }

        try {
            ProcessDataApplication app = new ProcessDataApplication();

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            app.doAnalytics(repository); //// readFromRocksDb

            stopWatch.stop();
            log.info("ProcessDataApplication Timing: {}", stopWatch);
        } catch (Exception e) {
            log.error("Can't ProcessDataApplication.doAnalytics with arrow from rocks", e);
        }


    }

    private void runFromConsole(String[] args) {
        switch (args.length) {
            case 2 -> {
                final byte[] key = args[1].getBytes();
                switch (args[0]) {
                    case "get" -> {
                        final Optional<byte[]> valueOpt = repository.find(key);
                        valueOpt
                                .map(SerializationUtils::deserialize)
                                .ifPresentOrElse(
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
                        final byte[] key = args[1].getBytes();
                        final byte[] value = SerializationUtils.serialize(args[2]);
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
