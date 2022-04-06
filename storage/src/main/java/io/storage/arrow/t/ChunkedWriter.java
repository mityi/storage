package io.storage.arrow.t;

import io.storage.arrow.RocksDbArrowWriter;
import io.storage.rocks.KVRepository;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ChunkedWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkedWriter.class);

    private final KVRepository<byte[], byte[]> repository;
    private final Vectorizer<T> vectorizer;


    public ChunkedWriter(KVRepository<byte[], byte[]> repository, Vectorizer<T> vectorizer) {
        this.repository = repository;
        this.vectorizer = vectorizer;
    }

    public void write(T[] values, Schema schema) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator);
             //todo
             RocksDbArrowWriter fileWriter = new RocksDbArrowWriter(repository, schemaRoot, dictProvider)) {

            LOGGER.info("Start writing");
            fileWriter.start();

            int index = 0;
            while (index < values.length) {
                schemaRoot.allocateNew();

                final int firstArrayElement = 0; //
                vectorizer.vectorize(values[index], firstArrayElement, schemaRoot);

                final int lengthArrayElement = 1; //
                schemaRoot.setRowCount(lengthArrayElement);

                LOGGER.info("Filled with item; {} item written", index);
                fileWriter.writeBatch();
                LOGGER.info("Chunk written");

                index++;
                schemaRoot.clear();
            }

            LOGGER.info("Writing done");
            fileWriter.end();
        }
    }

    @FunctionalInterface
    public interface Vectorizer<T> {
        void vectorize(T value, int index, VectorSchemaRoot batch);
    }
}
