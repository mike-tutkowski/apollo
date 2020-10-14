package com.datastax.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A common interface for data types
 */
public interface AbstractType<T> {
    int compare(T l, T r);
    void serialize(T value, DataOutput out) throws IOException;
    int sizeof(T value);
    T deserialize(DataInput in) throws IOException;
    boolean isFixedSize();
}
