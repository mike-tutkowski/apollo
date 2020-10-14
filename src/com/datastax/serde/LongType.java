package com.datastax.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongType implements AbstractType<Long> {

    public static LongType instance = new LongType();

    private LongType() { }

    @Override
    public int compare(Long l, Long r) {
        return Long.compare(l, r);
    }

    @Override
    public void serialize(Long value, DataOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public int sizeof(Long value) {
        return Long.BYTES;
    }

    @Override
    public Long deserialize(DataInput in) throws IOException {
        return in.readLong();
    }

    @Override
    public boolean isFixedSize() {
        return true;
    }
}
