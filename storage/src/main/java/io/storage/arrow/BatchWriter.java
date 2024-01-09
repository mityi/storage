package io.storage.arrow;

import io.storage.rocks.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BatchWriter<T> {


    private static final AtomicInteger id = new AtomicInteger(0);
    public static final byte[] firstKey = getBytes(0);

    private final KVRepository<byte[], byte[]> repository;
    private final Vectorizer<T> vectorizer;
    private final Schema schema;

    public BatchWriter(
            final KVRepository<byte[], byte[]> repository,
            final Vectorizer<T> vectorizer,
            final Schema schema) {
        this.repository = repository;
        this.vectorizer = vectorizer;
        this.schema = schema;
        {
            final Optional<byte[]> sizeKeyBytesOpt = repository.find(firstKey);

            sizeKeyBytesOpt.ifPresent(sizeKeyBytes -> {
                ByteBuffer wrapped = ByteBuffer.wrap(sizeKeyBytes); // big-endian by default
                int size = wrapped.getInt();
                id.set(size);
            });
        }

    }

    @NotNull
    private static byte[] getBytes(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(value);
        return bb.array();
    }

    public void writeBatch(T[] values) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        try (final RootAllocator allocator = new RootAllocator();
             final VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator);
             final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final ArrowStreamWriter fileWriter = new ArrowStreamWriter(schemaRoot, dictProvider, out)) {

            log.debug("Start writing");
            fileWriter.start();

            {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < values.length) {
                    vectorizer.vectorize(values[chunkIndex], chunkIndex, schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                log.debug("Filled chunk with {} items; {} items written", chunkIndex, chunkIndex);
                fileWriter.writeBatch();
                log.debug("Chunk written");

                schemaRoot.clear();
            }

            log.debug("Writing done");
            fileWriter.end();


            final byte[] byteArray = out.toByteArray();
            final int newId = id.incrementAndGet();
            final byte[] newIdBytes = getBytes(newId);
            repository.save(newIdBytes, byteArray);
            repository.save(firstKey, newIdBytes);
        }
    }

    @FunctionalInterface
    public interface Vectorizer<T> {
        void vectorize(T value, int index, VectorSchemaRoot batch);
    }
}
