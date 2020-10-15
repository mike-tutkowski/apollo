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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AsyncIteratorTest {
    private static final int NUMBER_OF_ROWS = 1000;

    @Test
    public void mergeIteratorTest() throws Throwable {
        Schema schema = new Schema.Builder()
                .addKey("key1", TextType.instance)
                .addKey("key2", IntType.instance)
                .addColumn("v1", TextType.instance)
                .addColumn("v2", LongType.instance)
                .build();

        TableIterator tableIterator1 = getTableIterator(schema);
        TableIterator tableIterator2 = getTableIterator(schema);

        Iterator<Row> mergeIterator = MergeIterator.create(Row::merge, tableIterator1, tableIterator2);

        int numberOfUniqueRows = 0;

        while (mergeIterator.hasNext()) {
            mergeIterator.next();

            numberOfUniqueRows++;
        }

        System.out.println("Number of Unique Rows = " + numberOfUniqueRows);

        Assert.assertEquals(numberOfUniqueRows, NUMBER_OF_ROWS);
    }

    @Test
    public void asyncIteratorTest() throws Throwable {
        Schema schema = new Schema.Builder()
                .addKey("key1", TextType.instance)
                .addKey("key2", IntType.instance)
                .addColumn("v1", TextType.instance)
                .addColumn("v2", LongType.instance)
                .build();

        TableIterator tableIterator1 = getTableIterator(schema);
        TableIterator tableIterator2 = getTableIterator(schema);

        Iterator<Row> mergeIterator = MergeIterator.create(Row::merge, tableIterator1, tableIterator2);

        AsyncIterator<Row> asyncIterator = new AsyncIterator<>(mergeIterator);

        int numberOfUniqueRows = 0;

        while (asyncIterator.hasNext()) {
            asyncIterator.next();

            numberOfUniqueRows++;
        }

        System.out.println("Number of Unique Rows = " + numberOfUniqueRows);

        Assert.assertEquals(numberOfUniqueRows, NUMBER_OF_ROWS);
    }

    private TableIterator getTableIterator(Schema schema) throws IOException {
        List<Row> rows = new ArrayList<>(NUMBER_OF_ROWS);

        for (int i = 0; i < NUMBER_OF_ROWS; i++) {
            Row row = createRowForTableIterator(schema, i);

            rows.add(row);
        }

        // ByteArrayOutputStream os = new ByteArrayOutputStream(4096);
        // DataOutput output = new DataOutputStream(os);
        DataOutput output = new DataOutputStream(new FileOutputStream("testfile.txt"));
        TableWriter writer = new TableWriter(schema, output);
        writer.write(rows.iterator());
        // os.close();

        // ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        // DataInput input = new DataInputStream(is);
        DataInput input = new DataInputStream(new FileInputStream("testfile.txt"));
        return new TableIterator(schema, input);
    }

    private Row createRowForTableIterator(Schema schema, int i1) {
        return Row.builder(schema)
                .addKey("key1", "ck" + i1)
                .addKey("key2", 1)
                .addColumn("v1", "v" + i1)
                .addColumn("v2", 1L)
                .row();
    }
}
