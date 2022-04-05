package io.storage.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowWriter;

import java.nio.channels.WritableByteChannel;

public class RocksDbArrowWriter extends ArrowWriter {

    public RocksDbArrowWriter(VectorSchemaRoot root,
                              DictionaryProvider provider,
                              WritableByteChannel out) {
        super(root, provider, out);
    }

}
