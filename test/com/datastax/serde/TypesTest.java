package com.datastax.serde;

import com.datastax.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TypesTest {

    @Test
    public void intTypeTest() throws IOException {
        for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; i+=100) {
            final int val = i;
            TestUtil.serDeTest(Integer.BYTES,
                               (out) -> IntType.instance.serialize(val, out),
                               (in) -> Assert.assertEquals((int) IntType.instance.deserialize(in), val));
        }
    }

    @Test
    public void longTypeTest() throws IOException {
        long step = Long.MAX_VALUE / 1000;
        for (long i = 0; i >= 0 && i < Long.MAX_VALUE; i+=step) {
            final long val = i;
            TestUtil.serDeTest(Integer.BYTES,
                               (out) -> LongType.instance.serialize(val, out),
                               (in) -> Assert.assertEquals((long) LongType.instance.deserialize(in), val));
        }
    }

    @Test
    public void textTypeTest() throws IOException {
        for (int i = 0; i < 10000; i++) {
            String val = TestUtil.randomString(i);
            TestUtil.serDeTest(Integer.BYTES,
                               (out) -> TextType.instance.serialize(val, out),
                               (in) -> Assert.assertEquals(TextType.instance.deserialize(in), val));
        }
    }

}