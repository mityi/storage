package io.storage.arrow.t;

import io.storage.arrow.RocksDbArrowWriter;
import io.storage.rocks.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

@Slf4j
public class ChunkedWriter<T> {

    private static final int chunkSize = 20_000;

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

            log.debug("Start writing");
            fileWriter.start();

            int index = 0;
            while (index < values.length) {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < chunkSize && index + chunkIndex < values.length) {
                    vectorizer.vectorize(values[index + chunkIndex], chunkIndex, schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                log.debug("Filled chunk with {} items; {} items written", chunkIndex, index + chunkIndex);
                fileWriter.writeBatch();
                log.debug("Chunk written");

                index += chunkIndex;
                schemaRoot.clear();
            }

            log.debug("Writing done");
            fileWriter.end();
        }
    }

    @FunctionalInterface
    public interface Vectorizer<T> {
        void vectorize(T value, int index, VectorSchemaRoot batch);
    }
}
