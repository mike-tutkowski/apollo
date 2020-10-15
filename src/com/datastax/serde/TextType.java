package com.datastax.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TextType implements AbstractType<String> {
    public static final int MAX_SERIALIZED_STRING_LENGTH = 2^31 + 1;

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

        if (sizeof(value) > MAX_SERIALIZED_STRING_LENGTH) {
            throw new IOException("The serializable size of a string must not exceed " +
                    MAX_SERIALIZED_STRING_LENGTH + " bytes.");
        }

        out.writeInt(value.getBytes().length);
        out.writeBytes(value);
    }

    @Override
    public int sizeof(String value) {
        // For the sake of simplicity, I am assuming the value of the variable 'value' is not null.

        return Integer.BYTES + value.getBytes().length;
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        // For the sake of simplicity, I am assuming the value of the variable 'in' is not null.

        int length = in.readInt();
        byte[] bytes = new byte[length];

        for (int i = 0; i < length; i++) {
            byte b = in.readByte();

            bytes[i] = b;
        }

        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}
