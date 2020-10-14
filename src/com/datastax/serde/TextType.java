package com.datastax.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextType implements AbstractType<String> {

    public static TextType instance = new TextType();

    private TextType() { }

    @Override
    public int compare(String l, String r) {
        return l.compareTo(r);
    }

    @Override
    public void serialize(String value, DataOutput out) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int sizeof(String value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isFixedSize() {
        throw new RuntimeException("Not implemented");
    }
}
