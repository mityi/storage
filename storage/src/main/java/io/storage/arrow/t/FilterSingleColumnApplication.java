package io.storage.arrow.t;

import io.storage.arrow.RocksDbArrowReader;
import io.storage.rocks.KVRepository;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.extern.slf4j.Slf4j;
import okio.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Small application to read chunked data from people.arrow file, and do some analytics on them:
 * - filter people living in a street ending with 'way'
 * - group by city
 * - aggregate average age
 */
@Slf4j
public class FilterSingleColumnApplication {


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
            Map<ByteString, Long> perCityCount = new TreeMap<>();
            Map<ByteString, Long> perCitySum = new TreeMap<>();
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
    private void processBatches(ArrowStreamReader reader, VectorSchemaRoot schemaRoot,
                                Map<ByteString, Long> perCityCount, Map<ByteString, Long> perCitySum)
            throws IOException {
        // Reading the data, one batch at a time
        while (reader.loadNextBatch()) {
            int[] selectedIndexes = filterOnStreet(schemaRoot).elements();

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
    private void aggregate(VectorSchemaRoot schemaRoot, int[] selectedIndexes, Map<ByteString, Long> perCityCount,
                           Map<ByteString, Long> perCitySum) {
        VarCharVector cityVector = (VarCharVector) ((StructVector) schemaRoot.getVector("address")).getChild("city");
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
        byte[] suffixInBytes = "way".getBytes();
        for (int i = 0; i < schemaRoot.getRowCount(); i++) {
            if (ByteString.of(streetVector.get(i)).endsWith(suffixInBytes)) {
                streetSelectedIndexes.add(i);
            }
        }
        streetSelectedIndexes.trim();
        return streetSelectedIndexes;
    }

}
