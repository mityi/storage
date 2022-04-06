package io.storage.arrow.t;

import io.storage.arrow.RocksDbArrowReader;
import io.storage.rocks.KVRepository;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.extern.slf4j.Slf4j;
import okio.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Small application to read chunked data from people.arrow file, and do some analytics on them:
 * - filter people having a last name starting with 'P'
 * - filter people between the age of 18 and 35
 * - filter people living in a street ending with 'way'
 * - group by city
 * - aggregate average age
 */
@Slf4j
public class ProcessDataApplication {


    /**
     * Main method: reading batches, filtering and aggregating.
     *
     * @throws IOException If reading from Arrow file fails
     */
    public void doAnalytics(KVRepository<byte[], byte[]> repository) throws IOException {
        RootAllocator allocator = new RootAllocator();

        try (RocksDbArrowReader reader = new RocksDbArrowReader(repository, allocator)) {
            VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();

            // Aggregate: Using ByteString as it is faster than creating a String from a byte[]
            Map<ByteString, Long> perCityCount = new HashMap<>();
            Map<ByteString, Long> perCitySum = new HashMap<>();
            processBatches(reader, schemaRoot, perCityCount, perCitySum);

            // Print results
            for (ByteString city : perCityCount.keySet()) {
                double average = (double) perCitySum.get(city) / perCityCount.get(city);
                log.info("City = {}; Average = {}", city, average);
            }
        }
    }

    /**
     * Read batches, apply filters and write aggregation values into aggregation data structures
     *
     * @param reader       Reads batches from Arrow file
     * @param schemaRoot   Schema root for read batches
     * @param perCityCount Aggregation of count per city
     * @param perCitySum   Aggregation of summed value per city
     * @throws IOException If reading the arrow file goes wrong
     */
    private void processBatches(RocksDbArrowReader reader,
                                VectorSchemaRoot schemaRoot,
                                Map<ByteString, Long> perCityCount,
                                Map<ByteString, Long> perCitySum) throws IOException {
        // Reading the data, one batch at a time
        while (reader.loadNextBatch()) {
            IntArrayList lastNameSelectedIndexes = filterOnLastName(schemaRoot);
            IntArrayList ageSelectedIndexes = filterOnAge(schemaRoot);
            IntArrayList streetSelectedIndexes = filterOnStreet(schemaRoot);

            IntArrayList lastNameAndAge = intersection(lastNameSelectedIndexes, ageSelectedIndexes);
            IntArrayList allIntersected = intersection(lastNameAndAge, streetSelectedIndexes);
            int[] selectedIndexes = allIntersected.elements();

            aggregate(schemaRoot, selectedIndexes, perCityCount, perCitySum);
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
    private void aggregate(VectorSchemaRoot schemaRoot,
                           int[] selectedIndexes,
                           Map<ByteString, Long> perCityCount,
                           Map<ByteString, Long> perCitySum) {
        VarCharVector cityVector = (VarCharVector) ((StructVector) schemaRoot.getVector("address")).getChild("city");
        UInt4Vector ageDataVector = (UInt4Vector) schemaRoot.getVector("age");

        for (int selectedIndex : selectedIndexes) {
            ByteString city = ByteString.of(cityVector.get(selectedIndex));
            perCityCount.put(city, perCityCount.getOrDefault(city, 0L) + 1);
            perCitySum.put(city, perCitySum.getOrDefault(city, 0L) + ageDataVector.get(selectedIndex));
        }
    }

    // Keep with last name starting with a P
    private IntArrayList filterOnLastName(VectorSchemaRoot schemaRoot) {
        VarCharVector lastName = (VarCharVector) schemaRoot.getVector("lastName");
        IntArrayList lastNameSelectedIndexes = new IntArrayList();
        byte[] prefixBytes = "P".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if (ByteString.of(lastName.get(i)).startsWith(prefixBytes)) {
                lastNameSelectedIndexes.add(i);
            }
        }
        lastNameSelectedIndexes.trim();
        return lastNameSelectedIndexes;
    }

    // Keep with age between 18 and 35
    private IntArrayList filterOnAge(VectorSchemaRoot schemaRoot) {
        UInt4Vector age = (UInt4Vector) schemaRoot.getVector("age");
        IntArrayList ageSelectedIndexes = new IntArrayList();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            int currentAge = age.get(i);
            if (18 <= currentAge && currentAge <= 35) {
                ageSelectedIndexes.add(i);
            }
        }
        ageSelectedIndexes.trim();
        return ageSelectedIndexes;
    }

    // Keep street ending in 'way'
    private IntArrayList filterOnStreet(VectorSchemaRoot schemaRoot) {
        StructVector addressVector = (StructVector) schemaRoot.getVector("address");
        VarCharVector streetVector = (VarCharVector) addressVector.getChild("street");

        IntArrayList streetSelectedIndexes = new IntArrayList();
        byte[] suffixBytes = "way".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if (ByteString.of(streetVector.get(i)).endsWith(suffixBytes)) {
                streetSelectedIndexes.add(i);
            }
        }
        streetSelectedIndexes.trim();
        return streetSelectedIndexes;
    }

    /**
     * Outputs intersection of two sorted integer arrays
     *
     * @param x first array
     * @param y second array
     * @return Intersection of x and y
     */
    private IntArrayList intersection(IntList x, IntList y) {
        int indexX = 0;
        int indexY = 0;
        IntArrayList intersection = new IntArrayList();

        while (indexX < x.size() && indexY < y.size()) {
            if (x.getInt(indexX) < y.getInt(indexY)) {
                indexX++;
            } else if (x.getInt(indexX) > y.getInt(indexY)) {
                indexY++;
            } else {
                intersection.add(x.getInt(indexX));
                indexX++;
                indexY++;
            }
        }

        intersection.trim();
        return intersection;
    }
}
