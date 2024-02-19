package io.storage;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DecimalUtility;
import org.rocksdb.RocksDB;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

class QueryProcessor {

    private final RocksDB rocksDB;
    private final BufferAllocator allocator;

    public QueryProcessor(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    public void processQuery(String query) {
        // Assuming a basic query structure
        try {
            final QueryParams queryParams = QueryParams.of(query);
            byte[] arrowData = rocksDB.get(queryParams.name().getBytes());
            if (arrowData != null) {
                processArrowData(arrowData, queryParams);
            } else {
                System.out.println("Collection not found: " + queryParams.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processArrowData(byte[] arrowData, QueryParams queryParams) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(arrowData);
             var reader = new ArrowStreamReader(bais, allocator)) {
            reader.loadNextBatch();
            try (final VectorSchemaRoot root = reader.getVectorSchemaRoot();) {

                final Schema schema = root.getSchema();
                final Field field = schema.findField(queryParams.path());
                if (field.getFieldType().getType() instanceof ArrowType.Utf8) {
                    VarCharVector vector = (VarCharVector) root.getVector(field);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (Arrays.equals(vector.get(i), queryParams.value.getBytes())) {
                            final int index = i;
                            schema.getFields().forEach(f -> {
                                if (f.getFieldType().getType() instanceof ArrowType.Utf8) {
                                    final VarCharVector vectorResult = (VarCharVector) root.getVector(f);
                                    if (!vectorResult.isNull(index)) {
                                        System.out.println(f.getName() + ": " + new String(vectorResult.get(index)));
                                    }
                                } else if (f.getFieldType().getType() instanceof ArrowType.Int) {
                                    final IntVector vectorResult = (IntVector) root.getVector(f);
                                    if (!vectorResult.isNull(index)) {
                                        System.out.println(f.getName() + ": " + vectorResult.get(index));
                                    }
                                } else if (f.getFieldType().getType() instanceof ArrowType.Bool) {
                                    final BitVector vectorResult = (BitVector) root.getVector(f);
                                    if (!vectorResult.isNull(index)) {
                                        System.out.println(f.getName() + ": " + vectorResult.get(index));
                                    }
                                } else if (f.getFieldType().getType() instanceof ArrowType.Decimal) {
                                    final DecimalVector vectorResult = (DecimalVector) root.getVector(f);
                                    if (!vectorResult.isNull(index)) {
                                        final ArrowBuf bytebuf = vectorResult.get(index);
                                        System.out.println(f.getName() + ": " +
                                                DecimalUtility.getBigDecimalFromArrowBuf(bytebuf,
                                                        0, vectorResult.getScale(), DecimalVector.TYPE_WIDTH)
                                        );
                                    }
                                }
                            });
                            System.out.println("-----");
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("test text");
                }
            }
        }
    }

    private record QueryParams(String name, String path, String value) {

        public static QueryParams of(String q) {
            final String[] split = q.split(" ", 3);
            return new QueryParams(split[0], split[1], split[2]);
        }

    }
}