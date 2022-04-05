package io.storage.arrow.t;

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

    static Schema personSchema() {
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
}
