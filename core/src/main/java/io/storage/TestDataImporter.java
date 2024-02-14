package io.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

class TestDataImporter {

    public static void importData(final RocksDB rocksDB, final String jsonFilePath) throws IOException, RocksDBException {
        final Path path = Path.of(jsonFilePath);
        if (Files.exists(path)) {
            print(path);
            byte[] arrowData = convertToArrowFormat(path);
            rocksDB.put("test".getBytes(), arrowData);
        }
    }

    private static void print(final Path path) throws IOException {
        final BufferedReader in = new BufferedReader(new FileReader(path.toFile()));
        String line = in.readLine();
        while (line != null) {
            System.out.println(line);
            line = in.readLine();
        }
        in.close();
    }

    private static byte[] convertToArrowFormat(final Path path) throws IOException {
        final var fields = new HashMap<String, Field>();
        {
            final JsonNode jsonArray = new ObjectMapper().readTree(path.toFile());
            if (jsonArray.isArray()) {
                for (var jsonNode : jsonArray) {
                    jsonNode.fields().forEachRemaining(jnEntry -> {
                        final String name = jnEntry.getKey();
                        final JsonNode jn = jnEntry.getValue();
                        if (jn.isBoolean()) {
                            fields.putIfAbsent(name, new Field(name, FieldType.nullable(new ArrowType.Bool()), null));
                        } else if (jn.isInt()) {
                            fields.putIfAbsent(name, new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null));
                        } else if (jn.isTextual()) {
                            fields.putIfAbsent(name, new Field(name, FieldType.nullable(new ArrowType.Utf8()), null));
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    });
                }
            }
        }
        final var schema = new Schema(fields.values());
        try (final RootAllocator allocator = new RootAllocator();
             final VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator);) {
            schemaRoot.allocateNew();

            // Serialize Arrow data to bytes

            final JsonNode jsonArray = new ObjectMapper().readTree(path.toFile());

            if (jsonArray.isArray()) {
                int i = 0;
                for (var jsonNode : jsonArray) {
                    final int index = i;
                    jsonNode.fields().forEachRemaining(jnEntry -> {
                        final String name = jnEntry.getKey();
                        final JsonNode jn = jnEntry.getValue();
                        final Field field = fields.get(name);
                        final FieldVector vector = schemaRoot.getVector(field);
                        if (jn.isBoolean()) {
                            ((BitVector) vector).setSafe(index, jn.asBoolean() ? 1 : 0);
                        } else if (jn.isNumber()) {
                            ((IntVector) vector).setSafe(index, Integer.valueOf(jn.asInt()).byteValue());
                        } else if (jn.isTextual()) {
                            ((VarCharVector) vector).setSafe(index, jn.asText().getBytes());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    });
                    i++;
                }
                schemaRoot.setRowCount(i);
            }
            final DictionaryProvider.MapDictionaryProvider dictProvider =
                    new DictionaryProvider.MapDictionaryProvider();
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final ArrowStreamWriter arrowFileWriter = new ArrowStreamWriter(schemaRoot, dictProvider, Channels.newChannel(baos));

            arrowFileWriter.start();
            arrowFileWriter.writeBatch();
            arrowFileWriter.end();

            return baos.toByteArray();
        }
    }

}