package io.storage.arrow.t;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class GenerateRandomDataApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateRandomDataApplication.class);
    private static final int CHUNK_SIZE = 20_000;

    /**
     * Generates an array of random people.
     *
     * @param numberOfPeople How many people to generate
     * @return Array of random people
     */
    Person[] randomPeople(int numberOfPeople) {
        Person[] people = new Person[numberOfPeople];

        for (int i = 0; i < numberOfPeople; i++) {
            people[i] = Person.randomPerson();
        }

        return people;
    }

    /**
     * Writes a given array of people into a file. The people are written out in chunks of size CHUNK_SIZE. The end
     * result is a bunch of bytes in a file called 'people.arrow'.
     * <p>
     * Writing in chunks has the advantage that only CHUNK_SIZE vectorized Person objects are pulled into memory
     * when reading the file from disk.
     *
     * @param people People to write to disk
     * @throws IOException Thrown if something goes wrong while writing to file 'people.arrow'.
     */
    void writeToArrowFile(Person[] people) throws IOException {
        new ChunkedWriter<>(CHUNK_SIZE, this::vectorizePerson)
                .write(new File("people.arrow"), people, ArrowSchemas.personSchema());
    }

    /**
     * Converts a Person into entries into the vector contained in the VectorSchemaRoot. The method assumes that the
     * schema of the vectorSchemaRoot is the Person schema.
     *
     * @param person     Person to write
     * @param index      Where to write in the vectors
     * @param schemaRoot Container of the vectors
     */
    private void vectorizePerson(Person person, int index, VectorSchemaRoot schemaRoot) {
        // Using setSafe: it increases the buffer capacity if needed
        ((VarCharVector) schemaRoot.getVector("firstName")).setSafe(index, person.getFirstName().getBytes());
        ((VarCharVector) schemaRoot.getVector("lastName")).setSafe(index, person.getLastName().getBytes());
        ((UInt4Vector) schemaRoot.getVector("age")).setSafe(index, person.getAge());

        List<FieldVector> childrenFromFields = schemaRoot.getVector("address").getChildrenFromFields();

        Address address = person.getAddress();
        ((VarCharVector) childrenFromFields.get(0)).setSafe(index, address.getStreet().getBytes());
        ((UInt4Vector) childrenFromFields.get(1)).setSafe(index, address.getStreetNumber());
        ((VarCharVector) childrenFromFields.get(2)).setSafe(index, address.getCity().getBytes());
        ((UInt4Vector) childrenFromFields.get(3)).setSafe(index, address.getPostalCode());
    }

    //========================================================================
    // Starting computation
    public static void main(String[] args) throws Exception {
        GenerateRandomDataApplication app = new GenerateRandomDataApplication();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int numberOfPeople = 10_047_031;
        LOGGER.info("Generating {} people", numberOfPeople);
        Person[] people = app.randomPeople(numberOfPeople);
        stopWatch.stop();
        stopWatch.start();
        LOGGER.info("Initiating writing");
        app.writeToArrowFile(people);
        stopWatch.stop();
        LOGGER.info("Timing: {}", stopWatch);
    }
}
