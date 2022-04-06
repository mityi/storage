package io.storage.arrow;

import io.storage.rocks.KVRepository;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDbArrowWriter extends ArrowStreamWriter {

    public RocksDbArrowWriter(KVRepository<byte[], byte[]> repository,
                              VectorSchemaRoot root,
                              DictionaryProvider provider) {

        super(root, provider, new RocksDbWritableByteChannel(repository));
    }

    private static class RocksDbWritableByteChannel implements WritableByteChannel {

        private final AtomicInteger ai = new AtomicInteger(0);
        private final KVRepository<byte[], byte[]> repository;

        public RocksDbWritableByteChannel(KVRepository<byte[], byte[]> repository) {
            this.repository = repository;
        }

        private byte[] intToBytes(final int i) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            return bb.array();
        }

        @Override
        public int write(ByteBuffer source) {

            final byte[] key = intToBytes(ai.getAndIncrement());
            final int length = source.remaining();
            final byte[] value = new byte[length];

            source.get(value, 0, length);

            final boolean save = repository.save(key, value);

            if (save) {
                return value.length;
            }
            return 0;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {

        }
    }
}
