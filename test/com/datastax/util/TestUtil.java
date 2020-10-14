package com.datastax.util;

import java.io.*;
import java.util.Random;

public class TestUtil {
    public static Random random;

    static
    {
        long seed = System.currentTimeMillis();
        System.out.println("seed = " + seed);
        random = new Random(seed);
    }

    public static void serDeTest(int size, IOConsumer<DataOutput> out, IOConsumer<DataInput> in) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(size);
        DataOutput output = new DataOutputStream(os);
        out.accept(output);
        os.close();

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        DataInput input = new DataInputStream(is);
        in.accept(input);
    }

    public static final String alphabet = "abcdefghijklmnopqrstuvwxyz";

    public static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i ++)
            sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
        return sb.toString();
    }

    public interface IOConsumer<T> {
        void accept(T t) throws IOException;
    }
}
