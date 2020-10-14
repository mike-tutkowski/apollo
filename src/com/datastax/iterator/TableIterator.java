package com.datastax.iterator;

import com.datastax.metadata.Constants;
import com.datastax.metadata.Schema;
import com.datastax.rows.Row;
import com.datastax.serde.MarshalException;

import java.io.*;
import java.nio.file.Files;
import java.util.Iterator;

/**
 * Synchronous variant of table / row iterator
 */
public class TableIterator implements Iterator<Row> {

    private final Schema schema;
    private final DataInput in;
    private boolean reachedEnd = false;

    public TableIterator(Schema schema, DataInput input) {
        this.schema = schema;
        this.in = input;
    }

    public boolean hasNext() {
        try {
            if (reachedEnd)
                return false;

            if (Constants.DEBUG) {
                int magic = in.readInt();
                if (magic == Constants.FILE_END)
                    return false;
                assert magic == Constants.ROW_MAGIC : "Can't deserialize row, ROW_MAGIC does not match " + magic + " != " + Constants.ROW_MAGIC;
            }

            return true;
        }
        catch (IOException e) {
            throw new MarshalException("Can't deserialize partition", e);
        }
    }

    public Row next() {
        try {
            // Might be useful for decoupling deserialization
            int serializedSize = in.readInt();

            Row row = Row.deserialize(schema, in);
            int separator = in.readInt();

            if (separator == Constants.FILE_END) {
                reachedEnd = true;
                return row;
            }

            assert separator == Constants.ROW_END: "Corrupted file: separator " + separator + " != " + Constants.ROW_END;
            return row;
        }
        catch (IOException e) {
            throw new MarshalException("Can't deserialize partition", e);
        }
    }

    public static OnDiskIterator onDiskIterator(Schema schema, String pathname) throws IOException {
        File file = new File(pathname);
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        return new OnDiskIterator(schema, in);
    }

    public static class OnDiskIterator extends TableIterator implements Closeable {

        private final DataInputStream stream;

        public OnDiskIterator(Schema schema, DataInputStream input) {
            super(schema, input);

            this.stream = input;
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }
}
