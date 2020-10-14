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
        // For the sake of simplicity, I am assuming that neither the variable 'value' nor the variable 'out'
        // has the value null.

        out.writeBytes(value);
    }

    @Override
    public int sizeof(String value) {
        // For the sake of simplicity, I am assuming the value of the variable 'value' is not null.

        return value.getBytes().length;
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        // For the sake of simplicity, I am assuming the value of the variable 'in' is not null.

        String line = in.readLine();

        return line != null ? line : "";
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}
