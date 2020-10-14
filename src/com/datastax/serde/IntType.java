package com.datastax.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntType implements AbstractType<Integer> {

    public static IntType instance = new IntType();

    public IntType() { }

    @Override
    public int compare(Integer l, Integer r) {
        return Integer.compare(l, r);
    }

    @Override
    public void serialize(Integer value, DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public int sizeof(Integer value) {
        return Integer.BYTES;
    }

    @Override
    public Integer deserialize(DataInput bb) throws IOException {
        return bb.readInt();
    }

    @Override
    public boolean isFixedSize() {
        return true;
    }
}
