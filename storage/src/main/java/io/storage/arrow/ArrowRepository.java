package io.storage.arrow;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ArrowRepository {


    public void saveComponent() {

    }

    private void vectorized(int index, Component component, VectorSchemaRoot schemaRoot) {
        ((VarCharVector) schemaRoot.getVector("key")).setSafe(index, component.key().getBytes());
        ((VarCharVector) schemaRoot.getVector("relatedKey")).setSafe(index, component.relatedKey().getBytes());
        ((VarCharVector) schemaRoot.getVector("relatedType")).setSafe(index, component.relatedType().getBytes());
        component.values(); //todo component.values();
    }
}
