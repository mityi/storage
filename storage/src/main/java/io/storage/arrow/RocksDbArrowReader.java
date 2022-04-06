package io.storage.arrow;

import io.storage.rocks.KVRepository;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDbArrowReader extends ArrowStreamReader {


    public RocksDbArrowReader(final KVRepository<byte[], byte[]> repository,
                              final BufferAllocator allocator
    ) {
        super(new RocksDbReadableByteChannel(repository), allocator);
    }

    private static class RocksDbReadableByteChannel implements ReadableByteChannel {
        private final AtomicInteger ai = new AtomicInteger(0);
        private final KVRepository<byte[], byte[]> repository;
        private byte[] head;
        private int readingLimit = 0;
        private int readingPos = 0;

        public RocksDbReadableByteChannel(KVRepository<byte[], byte[]> repository) {
            this.repository = repository;
            this.head = repository.find(intToBytes(ai.getAndIncrement())).orElse(null);
            if (this.head != null) {
                this.readingLimit = this.head.length;
                this.readingPos = 0;
            }
        }

        private byte[] intToBytes(final int i) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            return bb.array();
        }

        @Override
        public int read(ByteBuffer dst) {
            if (this.head == null) {
                return -1;
            } else {
                int remaining = dst.remaining();
                int current = readingLimit - readingPos;
                int toCopy = Math.min(remaining, current);
                dst.put(this.head, readingPos, toCopy);
                this.readingPos += toCopy;

                if (readingPos == readingLimit) {
                    this.head = repository.find(intToBytes(ai.getAndIncrement())).orElse(null);
                    if (this.head != null) {
                        this.readingLimit = this.head.length;
                        this.readingPos = 0;
                    }
                }
                return toCopy;
            }
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
