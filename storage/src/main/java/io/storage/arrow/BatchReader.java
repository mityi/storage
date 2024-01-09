package io.storage.arrow;

import io.storage.rocks.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public class BatchReader {

    private static final AtomicInteger id = new AtomicInteger(0);

    private final KVRepository<byte[], byte[]> repository;
    private final Consumer<VectorSchemaRoot> consume;

    public BatchReader(
            final KVRepository<byte[], byte[]> repository,
            final Consumer<VectorSchemaRoot> consume) {
        this.repository = repository;
        this.consume = consume;
        {
            final int sizeId = 0;
            final byte[] sizeKey = getBytes(sizeId);
            final Optional<byte[]> sizeKeyBytesOpt = repository.find(sizeKey);

            sizeKeyBytesOpt.ifPresent(sizeKeyBytes -> {
                ByteBuffer wrapped = ByteBuffer.wrap(sizeKeyBytes); // big-endian by default
                int size = wrapped.getInt();
                id.set(size);
            });
        }

    }

    @NotNull
    private byte[] getBytes(int value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(value);
        return bb.array();
    }

    public void readBatchesInParallel() {
        final ExecutorCompletionService<Void> completionService =
                //todo we need to speed up with green thread or have flexibility in configurations
                new ExecutorCompletionService<>(Executors.newFixedThreadPool(8));

        int count = id.get();

        while (id.get() != 0) {
            final int id = BatchReader.id.getAndDecrement();
            final Optional<byte[]> bytesOpt = repository.find(getBytes(id));
            completionService.submit(() -> {
                try (RootAllocator allocator = new RootAllocator()) {
                    bytesOpt.ifPresent(bytes -> {
                        try (final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                             ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
                            final VectorSchemaRoot schema = reader.getVectorSchemaRoot();
                            while (reader.loadNextBatch()) {
                                consume.accept(schema);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                return null;
            });
        }
        try {
            while (count != 0) {
                completionService.take().get();
                count--;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
