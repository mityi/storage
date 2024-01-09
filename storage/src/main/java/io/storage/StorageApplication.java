package io.storage;

//import com.google.common.base.Stopwatch;
import io.storage.arrow.BatchReader;
import io.storage.arrow.BatchWriter;
import io.storage.arrow.RocksDbArrowReader;
import io.storage.arrow.t.ArrowSchemas;
import io.storage.arrow.t.FilterSingleColumnApplication;
import io.storage.arrow.t.GenerateRandomDataApplication;
import io.storage.arrow.t.Person;
import io.storage.arrow.t.ProcessDataApplication;
import io.storage.rocks.KVRepository;
import io.storage.rocks.RocksDBRepository;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okio.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@SpringBootApplication
public class StorageApplication implements CommandLineRunner {
    private final KVRepository<byte[], byte[]> repository = new RocksDBRepository();

    public static void main(String[] args) throws IOException {
        log.info("Starting the storage");

        SpringApplication.run(StorageApplication.class, args);
        log.info("Storage finished");
    }

    @Override
    public void run(String... args) throws IOException {
        log.info("EXECUTING : command line runner");

        if (true) {
            int numberOfPeople = 10_047_031;
//            int numberOfPeople = 42_000;
            log.info("Generating {} people", numberOfPeople);
            Person[] people = randomPeople(numberOfPeople);
            try {
                GenerateRandomDataApplication app = new GenerateRandomDataApplication();

                StopWatch stopWatch = new StopWatch();
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

        if (false) {
            try (RootAllocator allocator = new RootAllocator()) {

                try (RocksDbArrowReader reader = new RocksDbArrowReader(repository, allocator)) {
                    final VectorSchemaRoot schema = reader.getVectorSchemaRoot();

                    Map<ByteString, Long> perCityCount = new HashMap<>();
                    Map<ByteString, Long> perCitySum = new HashMap<>();

                    while (reader.loadNextBatch()) {
                        int[] selectedIndexes = filterOnStreet(schema).elements();
                        aggregate(schema, selectedIndexes, perCityCount, perCitySum);
                    }

                    // Print results
                    for (ByteString city : perCityCount.keySet()) {
                        double average = (double) perCitySum.get(city) / perCityCount.get(city);
                        log.info("City = {}; Average = {}", city, average);
                    }
                }
            }
        }

        if (true) {
            int numberOfPeople = 42;
            log.info("Generating {} people", numberOfPeople);
            Person[] people = randomPeople(numberOfPeople);

            BatchWriter<Person> writer = new BatchWriter<>(
                    repository,
                    ArrowSchemas::vectorizePerson,
                    ArrowSchemas.personSchema());

            if (people.length > 20_000){
                int index = 0;
                while (index < people.length) {
                    int chunkIndex = 0;
                    final Person[]  peopleChunk = new Person[
                            Math.min(people.length - index + chunkIndex, 20_000)
                            ];
                    while (chunkIndex < 20_000 && index + chunkIndex < people.length) {
                        peopleChunk[chunkIndex] = people[index + chunkIndex];
                        chunkIndex++;
                    }
                    writer.writeBatch(peopleChunk);

                    index += chunkIndex;
                }
            } else {
                writer.writeBatch(people);
            }
            log.info("Written {} people", numberOfPeople);

            Map<ByteString, Long> perCityCount = new ConcurrentHashMap<>();
            Map<ByteString, Long> perCitySum = new ConcurrentHashMap<>();

            BatchReader reader = new BatchReader(
                    repository,
                    schema -> {
                        int[] selectedIndexes = filterOnStreet(schema).elements();
                        aggregate(schema, selectedIndexes, perCityCount, perCitySum);
                    });
            reader.readBatchesInParallel();
            // Print results
            for (ByteString city : perCityCount.keySet()) {
                double average = (double) perCitySum.get(city) / perCityCount.get(city);
                log.info("City = {}; Average = {}", city, average);
            }

        }
    }

    /**
     * Given the selected indexes, it copies the aggregation values into aggregation vectors
     *
     * @param schemaRoot      Schema root of batch
     * @param selectedIndexes Indexes to aggregate
     * @param perCityCount    Aggregating counts per city
     * @param perCitySum      Aggregating sums per city
     */
    private void aggregate(VectorSchemaRoot schemaRoot, int[] selectedIndexes, Map<ByteString, Long> perCityCount,
                           Map<ByteString, Long> perCitySum) {
        VarCharVector cityVector = (VarCharVector) ((StructVector) schemaRoot.getVector("address"))
                .getChild("city");
        UInt4Vector ageDataVector = (UInt4Vector) schemaRoot.getVector("age");

        for (int selectedIndex : selectedIndexes) {
            ByteString city = ByteString.of(cityVector.get(selectedIndex));
            perCityCount.put(city, perCityCount.getOrDefault(city, 0L) + 1);
            perCitySum.put(city, perCitySum.getOrDefault(city, 0L) + ageDataVector.get(selectedIndex));
        }
    }

    // Keep street ending in 'way'
    private IntArrayList filterOnStreet(VectorSchemaRoot schemaRoot) {
        StructVector addressVector = (StructVector) schemaRoot.getVector("address");
        VarCharVector streetVector = (VarCharVector) addressVector.getChild("street");

        IntArrayList streetSelectedIndexes = new IntArrayList();
        byte[] suffixInBytes = "way" .getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if (ByteString.of(streetVector.get(i)).endsWith(suffixInBytes)) {
                streetSelectedIndexes.add(i);
            }
        }
        streetSelectedIndexes.trim();
        return streetSelectedIndexes;
    }

    /**
     * Generates an array of random people.
     *
     * @param numberOfPeople How many people to generate
     * @return Array of random people
     */
    public Person[] randomPeople(int numberOfPeople) {
        Person[] people = new Person[numberOfPeople];

        for (int i = 0; i < numberOfPeople; i++) {
            people[i] = Person.randomPerson();
        }

        return people;
    }
}
