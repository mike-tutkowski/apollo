package com.datastax.iterator;

import com.datastax.metadata.Schema;
import com.datastax.rows.Row;
import com.datastax.serde.TextType;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.function.BiFunction;

public class MergeIteratorTest {
    private final static BiFunction<Row, Row, Row> ROW_MERGER = Row::merge;

    private static final Schema schema = new Schema.Builder()
            .addKey("key", TextType.instance)
            .addColumn("value", TextType.instance)
            .build();

    private Row row(long ts, String key, String v) {
        return Row.builder(schema, ts)
                .addKey("key", key)
                .addColumn("value", v)
                .row();
    }

    @Test
    public void testMergeIterator() throws Schema.AlreadyExistsException {
        Iterator<Row> iter1 = Arrays.asList(row(3,"key001", "v01"),
                                            row(1,"key002", "v01"),
                                            row(1,"key003", "v01")).iterator();

        Iterator<Row> iter2 = Arrays.asList(row(2,"key001", "v02"),
                                            row(10,"key004","v02")).iterator();

        Iterator<Row> iter3 = Arrays.asList(row(1,"key001","v03"),
                                            row(1,"key002","v03"),
                                            row(100,"key003","v04")).iterator();


        Iterator<Row> rowMergeIterator = MergeIterator.create(ROW_MERGER,
                                                              iter1,
                                                              iter2,
                                                              iter3);

        Assert.assertEquals(rowMergeIterator.next(), row(3,"key001", "v01"));
        Assert.assertEquals(rowMergeIterator.next(), row(1,"key002", "v01"));
        Assert.assertEquals(rowMergeIterator.next(), row(100,"key003", "v04"));
        Assert.assertEquals(rowMergeIterator.next(), row(10,"key004", "v02"));

        Assert.assertFalse(rowMergeIterator.hasNext());
    }

    @Test
    public void stressTestMergeIterator() throws Schema.AlreadyExistsException
    {
        int iteratorCount = 100;
        int iterations = 100000;
        List<Row>[] rowIteratorLists = new List[iteratorCount];
        Random rnd = new Random();
        for (int i = 0; i < iterations; i++) {
            int iterIdx = rnd.nextInt(iteratorCount);
            if (rowIteratorLists[iterIdx] == null)
                rowIteratorLists[iterIdx] = new ArrayList<>();

            rowIteratorLists[iterIdx].add(row(1L, String.format("key%5d", i),  String.format("v%5d", i)));
        }

        Iterator<Row>[] iterators = new Iterator[iteratorCount];
        for (int i = 0; i < iteratorCount; i++) {
            iterators[i] = rowIteratorLists[i].iterator();
        }

        Iterator<Row> mergeIterator = SortedInvariantCheckIterator.wrap(MergeIterator.create(ROW_MERGER,
                                                                                             iterators));

        int i = 0;
        while (mergeIterator.hasNext()) {
            Assert.assertEquals(mergeIterator.next(),
                                row(1L, String.format("key%5d", i),  String.format("v%5d", i)));
            i++;
        }
        Assert.assertFalse(mergeIterator.hasNext());
        Assert.assertEquals(iterations, i);
    }
}
