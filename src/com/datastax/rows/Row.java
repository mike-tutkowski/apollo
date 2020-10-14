package com.datastax.rows;

import com.datastax.metadata.Schema;
import com.datastax.serde.AbstractType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Row is a single instance of data within table. Row consists of
 * {@link Row#key}, {@link Row#writeTime} and {@link Row#rowColumns},
 * values mapped to the supplied key.
 *
 * In order to construct a row, {@link Schema} has to be supplied
 * and {@link Row.RowBuilder} has to be used.
 *
 * Row can be serialized (with {@link Row#serialize(DataOutput)})
 * and deserialized (with {@link Row#deserialize(Schema, DataInput)}).
 */
public final class Row implements Comparable<Row> {

    private final Object[] key;
    private final long writeTime;
    private final Schema schema;
    private final Map<Schema.Column, Object> rowColumns;

    private Row(Schema schema,
                long writeTime,
                Object[] key,
                Map<Schema.Column, Object> rowColumns) {
        this.schema = schema;
        this.writeTime = writeTime;
        this.key = key;
        this.rowColumns = rowColumns;
    }

    public long writeTime() {
        return writeTime;
    }

    public boolean hasColumn(Schema.Column column) {
        return rowColumns.containsKey(column);
    }

    public Object getColumn(Schema.Column column) {
        return rowColumns.get(column);
    }

    public static Row merge(Row l, Row r) {
        assert l.schema.equals(r.schema) : "Can't merge rows of different schema.";
        assert Arrays.equals(l.key, r.key) : "Can't merge rows of different clusterings";

        return l.writeTime() >= r.writeTime() ? l : r;
    }

    @Override
    public int compareTo(Row o) {
        assert this.key.length == o.key.length : "Can't compare different clusterings";
        assert this.schema == o.schema;

        for (int i = 0; i < key.length; i++) {
            AbstractType type = schema.getClusteringKey(i).type;
            int res = type.compare(key[i], o.key[i]);
            if (res != 0)
                return res;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return writeTime == row.writeTime &&
                Arrays.equals(key, row.key) &&
                Objects.equals(schema, row.schema) &&
                Objects.equals(rowColumns, row.rowColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, writeTime, schema, rowColumns);
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeLong(writeTime);

        int i = 0;
        for (Schema.Column<?> c : schema.clusteringKeyColumns()) {
            c.type.serialize(key[i], out);
            i++;
        }

        int rowsBitmap = 0x0;
        i = 0;
        for (Schema.Column<?> column : schema.rowColumns()) {
            if (hasColumn(column)) {
                rowsBitmap |= (1 << i);
            }
            i++;
        }
        out.writeInt(rowsBitmap);

        for (Schema.Column<?> c : schema.rowColumns()) {
            c.type.serialize(getColumn(c), out);
        }
    }

    public int serializedSize() {
        int size = 0;

        // writetime
        size += Long.BYTES;

        int i = 0;
        // columns
        for (Schema.Column<?> c : schema.clusteringKeyColumns())
            size += c.type.sizeof(key[i++]);

        // rows bitmap
        size += Integer.BYTES;

        // values
        for (Schema.Column<?> c : schema.rowColumns())
            size += c.type.sizeof(getColumn(c));

        return size;
    }

    public static Row deserialize(Schema schema, DataInput in) throws IOException {
        final long writeTime = in.readLong();

        Object[] clusteringKey = new Object[schema.clusteringKeyColumns().size()];
        Map<Schema.Column, Object> columns = new HashMap<>();

        int i = 0;
        for (Schema.Column<?> c : schema.clusteringKeyColumns()) {
            clusteringKey[i++] = c.type.deserialize(in);
        }

        final int rowsBitmap = in.readInt();
        i = 0;
        for (Schema.Column<?> c : schema.rowColumns()) {
            if ((rowsBitmap & (1 << i)) != 0){
                columns.put(c, c.type.deserialize(in));
            }
            i++;
        }

        return new Row(schema, writeTime, clusteringKey, columns);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Row(").append("ts=").append(writeTime).append(" ");

        b.append("[");
        int i = 0;
        for (Schema.Column<?> column : schema.clusteringKeyColumns()) {
            b.append(" ")
                    .append(column.name)
                    .append(" = ")
                    .append(key[i++]);
        }
        b.append("]");

        for (Schema.Column<?> column : schema.rowColumns()) {
            b.append(" ")
                    .append(column.name)
                    .append(" = ")
                    .append(getColumn(column));
        }

        b.append(")");
        return b.toString();
    }

    public static RowBuilder builder(Schema schema, long writeTime) {
        return new RowBuilder(schema, writeTime);
    }

    public static RowBuilder builder(Schema schema) {
        return new RowBuilder(schema);
    }

    /**
     * Helper class to construct instances of Row
     */
    public static class RowBuilder {

        final Map<Schema.Column, Object> clusteringKey;
        final Map<Schema.Column, Object> columns;
        final Schema schema;
        final long writeTime;

        private RowBuilder(Schema schema) {
            this(schema, System.currentTimeMillis());
        }

        private RowBuilder(Schema schema, long writeTime) {
            this.schema = schema;
            this.clusteringKey =  new HashMap<>();
            this.columns = new HashMap<>();
            this.writeTime = writeTime;
        }

        public RowBuilder addKey(String columnName, Object value) {
            Schema.Column<?> column = schema.getColumn(columnName);
            assert column.columnType == Schema.ColumnType.CLUSTERING_KEY : "Column " + columnName + " is not a part of clustering key";
            this.clusteringKey.put(column, value);
            return this;
        }

        public RowBuilder addColumn(String columnName, Object value) {
            Schema.Column<?> column = schema.getColumn(columnName);
            assert column.columnType == Schema.ColumnType.ROW_COLUMN : "Column " + columnName + " is not a row column";
            this.columns.put(column, value);
            return this;
        }

        public Row row() {
            for (Schema.Column<?> c : schema.clusteringKeyColumns()) {
                assert clusteringKey.containsKey(c) : "Can't build a row without Clustering Key " + c.name;
            }

            return new Row(schema, writeTime, makeClustering(), columns);
        }

        private Object[] makeClustering() {
            Object[] arr = new Object[schema.clusteringKeyColumns().size()];
            int i = 0;
            for (Schema.Column<?> column : schema.clusteringKeyColumns()) {
                arr[i++] = clusteringKey.get(column);
            }
            return arr;
        }
    }


}
