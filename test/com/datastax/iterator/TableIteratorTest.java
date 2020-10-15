package com.datastax.iterator;

import com.datastax.metadata.Schema;
import com.datastax.rows.Row;
import com.datastax.serde.IntType;
import com.datastax.serde.LongType;
import com.datastax.serde.TextType;
import com.datastax.writer.TableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

public class TableIteratorTest {

    @Test
    public void tableIteratorTest() throws Throwable {
        Schema schema = new Schema.Builder()
                .addKey("key1", TextType.instance)
                .addKey("key2", IntType.instance)
                .addColumn("v1", TextType.instance)
                .addColumn("v2", LongType.instance)
                .build();

        Row row1 = Row.builder(schema)
                .addKey("key1", "ck001")
                .addKey("key2", 1)
                .addColumn("v1", "v01")
                .addColumn("v2", 1L)
                .row();

        Row row2 = Row.builder(schema)
                .addKey("key1", "ck002")
                .addKey("key2", 1)
                .addColumn("v1", "v02")
                .addColumn("v2", 2L)
                .row();

        Row row3 = Row.builder(schema)
                .addKey("key1", "ck003")
                .addKey("key2", 1)
                .addColumn("v1", "v03")
                .addColumn("v2", 3L)
                .row();

        ByteArrayOutputStream os = new ByteArrayOutputStream(4096);
        DataOutput output = new DataOutputStream(os);
        TableWriter writer = new TableWriter(schema, output);
        writer.write(Arrays.asList(row1,
                                   row2,
                                   row3).iterator());
        os.close();

        // debug output
        // System.out.println(ByteBufferUtil.prettyHexDump(ByteBuffer.wrap(os.toByteArray())));

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        DataInput input = new DataInputStream(is);
        TableIterator iter = new TableIterator(schema, input);

        Assert.assertTrue(iter.hasNext());
        Row deserialized = iter.next();
        Assert.assertEquals(row1, deserialized);

        Assert.assertTrue(iter.hasNext());
        deserialized = iter.next();
        Assert.assertEquals(row2, deserialized);

        Assert.assertTrue(iter.hasNext());
        deserialized = iter.next();
        Assert.assertEquals(row3, deserialized);
        Assert.assertFalse(iter.hasNext());
    }
}
