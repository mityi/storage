package io.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

class InsertProcessor {

    private final RocksDB rocksDB;
    private final BufferAllocator allocator;
    private final BufferAllocator allocatorRead;

    public InsertProcessor(final RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.allocatorRead = new RootAllocator(Long.MAX_VALUE);
    }

    public void insertData(final String collection, final String jsonDate) throws IOException, RocksDBException {
        byte[] bytes = rocksDB.get(collection.getBytes());
        byte[] arrowData = convertToArrowFormat(bytes, jsonDate);
        rocksDB.put(collection.getBytes(), arrowData);
    }

    private byte[] convertToArrowFormat(byte[] bytes, final String data) throws IOException {
        ;
        if (bytes == null) {
            final VectorSchemaRoot root = convertToVectorSchemaRoot(null, data);
            return toByte(root);
        } else {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 var reader = new ArrowStreamReader(bais, allocatorRead)) {
                var dbData = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                final VectorSchemaRoot root = convertToVectorSchemaRoot(dbData, data);
                return toByte(root);
            }
        }
    }

    private static byte[] toByte(VectorSchemaRoot root) throws IOException {
        final DictionaryProvider.MapDictionaryProvider dictProvider =
                new DictionaryProvider.MapDictionaryProvider();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final ArrowStreamWriter arrowFileWriter = new ArrowStreamWriter(root, dictProvider, Channels.newChannel(baos));

        arrowFileWriter.start();
        arrowFileWriter.writeBatch();
        arrowFileWriter.end();

        return baos.toByteArray();
    }

    private VectorSchemaRoot convertToVectorSchemaRoot(
            @Nullable final VectorSchemaRoot root, final String data) throws JsonProcessingException {
        int index = Optional.ofNullable(root)
                .map(VectorSchemaRoot::getRowCount)
                .orElse(0);

        final var vectors = new HashMap<String, FieldVector>();

        final JsonNode jsonArray = new ObjectMapper().readTree(data);
        if (jsonArray.isArray()) {
            for (var jsonNode : jsonArray) {
                for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
                    final var jnEntry = it.next();
                    final String name = jnEntry.getKey();
                    final JsonNode jn = jnEntry.getValue();

                    final Optional<FieldVector> fvOpt = Optional.ofNullable(root)
                            .map(r -> r.getVector(name))
                            .or(() -> Optional.ofNullable(vectors.get(name)));
                    if (jn.isBoolean()) {
                        BitVector bitVector = fvOpt
                                .map(vv -> (BitVector) vv)
                                .orElseGet(() -> {
                                    final var newVector = new BitVector(
                                            new Field(name, FieldType.nullable(
                                                    new ArrowType.Bool()), null),
                                            allocator);
                                    vectors.put(name, newVector);
                                    return newVector;
                                });

                        bitVector.setSafe(index, jn.asBoolean() ? 1 : 0);
                        bitVector.setValueCount(index + 1);

                    } else if (jn.isInt()) {
                        IntVector intVector = fvOpt
                                .map(vv -> (IntVector) vv)
                                .orElseGet(() -> {
                                    final var newVector = new IntVector(
                                            new Field(name, FieldType.nullable(
                                                    new ArrowType.Int(32, true)), null),
                                            allocator);
                                    vectors.put(name, newVector);
                                    return newVector;
                                });

                        intVector.setSafe(index, Integer.valueOf(jn.asInt()).byteValue());
                        intVector.setValueCount(index + 1);

                    } else if (jn.isTextual()) {
                        VarCharVector stringVector = fvOpt
                                .map(vv -> (VarCharVector) vv)
                                .orElseGet(() ->
                                {
                                    final var newVector = new VarCharVector(
                                            new Field(name, FieldType.nullable(
                                                    new ArrowType.Utf8()), null),
                                            allocator);
                                    vectors.put(name, newVector);
                                    return newVector;
                                });
                        stringVector.setSafe(index, jn.asText().getBytes());
                        stringVector.setValueCount(index + 1);

                    } else if (jn.isFloatingPointNumber()) {
                        DecimalVector decimalVector = fvOpt
                                .map(vv -> (DecimalVector) vv)
                                .orElseGet(() ->
                                {
                                    final var newVector = new DecimalVector(
                                            new Field(name, FieldType.nullable(
                                                    new ArrowType.Decimal(15, 3, 128)), null),
                                            allocator);
                                    vectors.put(name, newVector);
                                    return newVector;
                                });
                        decimalVector.setSafe(index, jn.decimalValue().setScale(3, RoundingMode.HALF_UP));
                        decimalVector.setValueCount(index + 1);

                    } else {
                        throw new UnsupportedOperationException();
                    }
                }
                index++;
            }
        }

        final VectorSchemaRoot result = Optional.ofNullable(root)
                .map(r -> {
                    VectorSchemaRoot newVsr = r;
                    for (FieldVector fv : vectors.values()) {
                        newVsr = newVsr.addVector(0, fv);
                    }
                    return newVsr;
                })
                .orElseGet(() -> new VectorSchemaRoot(vectors.values()));

        result.setRowCount(index);
        return result;
    }

}
