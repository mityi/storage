package io.storage.arrow.t;

import io.storage.rocks.KVRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.List;

@Slf4j
public class GenerateRandomDataApplication {


    /**
     * Writes a given array of people into a file. The people are written out in chunks of size CHUNK_SIZE. The end
     * result is a bunch of bytes in a file called 'people.arrow'.
     * <p>
     * Writing in chunks has the advantage that only CHUNK_SIZE vectorized Person objects are pulled into memory
     * when reading the file from disk.
     *
     * @param people     People to write to disk
     * @param repository
     * @throws IOException Thrown if something goes wrong while writing to file 'people.arrow'.
     */
    public void writeToArrowFile(Person[] people, KVRepository<byte[], byte[]> repository) throws IOException {
//        new ChunkedWriter<>(CHUNK_SIZE, this::vectorizePerson)
//                .write(new File("people.arrow"), people, ArrowSchemas.personSchema());
        new ChunkedWriter<>(repository, ArrowSchemas::vectorizePerson)
                .write(people, ArrowSchemas.personSchema());

    }



}
