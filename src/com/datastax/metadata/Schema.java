package com.datastax.metadata;

import com.datastax.serde.AbstractType;

import java.util.*;
import java.util.function.Consumer;

/**
 * Database Schema: describes key and column parts of the given table.
 *
 * This simplified storage system can hold tables having a multi-part key
 * mapped to multiple values.
 *
 * Each key or column has a name ({@link String}) and type (subclasses of
 * {@link AbstractType}).
 *
 * Use {@link Schema.Builder} to create instances of Schema.
 */
public class Schema {
    private final List<Column<?>> keyColumns;
    private final List<Column<?>> rowColumns;
    private final Map<String, Column<?>> columnMap;

    private Schema(List<Column<?>> keyColumns,
                   List<Column<?>> rowColumns,
                   Map<String, Column<?>> columnMap) {
        this.keyColumns = keyColumns;
        this.rowColumns = rowColumns;
        this.columnMap = columnMap;
    }

    public Collection<Column<?>> clusteringKeyColumns() {
        return keyColumns;
    }

    public Column<?> getClusteringKey(int i) {
        return keyColumns.get(i);
    }

    public Iterable<Column<?>> rowColumns() {
        return rowColumns;
    }

    public void withRowColumns(Consumer<Column<?>> consumer) {
        rowColumns.forEach(consumer);
    }

    public Column<?> getColumn(String columnName) {
        return columnMap.get(columnName);
    }

    public static class Builder {
        private final Map<String, Column<?>> columnMap;
        private final List<Column<?>> clusteringKeyColumns;
        private final List<Column<?>> rowColumns;

        public Builder() {
            this.columnMap = new HashMap<>();
            this.clusteringKeyColumns = new ArrayList<>();
            this.rowColumns = new ArrayList<>();
        }

        public Builder addKey(String name, AbstractType type) {
            if (this.columnMap.containsKey(name))
                throw new AlreadyExistsException("Column " + name + " is already present in schema");

            Column<?> column = new Column<>(name, ColumnType.CLUSTERING_KEY, type);
            this.columnMap.put(name, column);
            this.clusteringKeyColumns.add(column);
            return this;
        }

        public Builder addColumn(String name, AbstractType type) {
            if (this.columnMap.containsKey(name))
                throw new AlreadyExistsException("Column " + name + " is already present in schema");

            Column<?> column = new Column<>(name, ColumnType.ROW_COLUMN, type);
            this.columnMap.put(name, column);
            this.rowColumns.add(column);
            return this;
        }

        public Schema build() {
            return new Schema(clusteringKeyColumns,
                              rowColumns,
                              columnMap);
        }
    }

    public enum ColumnType {
        CLUSTERING_KEY,
        ROW_COLUMN
    }

    public static class Column<T> implements Comparable<Column> {
        public final String name;
        public final AbstractType type;
        public final ColumnType columnType;

        public Column(String name, ColumnType columnType, AbstractType type) {
            this.name = name;
            this.type = type;
            this.columnType = columnType;
        }

        @Override
        public int compareTo(Column o) {
            return name.compareTo(o.name);
        }

        @Override
        public String toString() {
            return "Column(" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", columnType=" + columnType +
                    ')';
        }
    }

    public static class AlreadyExistsException extends RuntimeException {
        public AlreadyExistsException(String message) {
            super(message);
        }
    }
}
