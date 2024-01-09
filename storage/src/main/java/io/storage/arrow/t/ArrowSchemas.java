package io.storage.arrow.t;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static java.util.Arrays.asList;

public class ArrowSchemas {

    static Schema addressSchema() {
        return new Schema(addressFields());
    }

    public static Schema personSchema() {
        return new Schema(personFields());
    }

    private static List<Field> addressFields() {
        return asList(
                new Field("street", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("streetNumber", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("city", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("postalCode", FieldType.nullable(new ArrowType.Int(32, false)), null)
        );
    }

    private static List<Field> personFields() {
        return asList(
                new Field("firstName", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("lastName", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(32, false)), null),
                new Field("address", FieldType.nullable(new ArrowType.Struct()), addressFields())
        );
    }

    /**
     * Converts a Person into entries into the vector contained in the VectorSchemaRoot. The method assumes that the
     * schema of the vectorSchemaRoot is the Person schema.
     *
     * @param person     Person to write
     * @param index      Where to write in the vectors
     * @param schemaRoot Container of the vectors
     */
    public static void vectorizePerson(Person person, int index, VectorSchemaRoot schemaRoot) {
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
}
