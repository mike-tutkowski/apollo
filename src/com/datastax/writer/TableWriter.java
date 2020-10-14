package com.datastax.writer;

import com.datastax.metadata.Constants;
import com.datastax.metadata.Schema;
import com.datastax.rows.Row;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Helper class to write tables / sequences of rows
 */
public class TableWriter {

    private final Schema schema;
    private final DataOutput out;

    public TableWriter(Schema schema, DataOutput output) {
        this.schema = schema;
        this.out = output;
    }

    public void write(Iterator<Row> iterator) throws IOException {
        boolean isFirst = true;
        while (iterator.hasNext()) {
            if (isFirst)
                isFirst = false;
            else
                out.writeInt(Constants.ROW_END);

            if (Constants.DEBUG)
                out.writeInt(Constants.ROW_MAGIC);
            Row row = iterator.next();
            out.writeInt(row.serializedSize());
            row.serialize(out);
        }

        out.writeInt(Constants.FILE_END);
    }

    public static OnDiskWriter onDiskWriter(Schema schema, Path pathname) throws IOException {
        return new OnDiskWriter(schema, new DataOutputStream(Files.newOutputStream(pathname)));
    }
    
    public static class OnDiskWriter extends TableWriter implements Closeable {

        private final DataOutputStream out;
        public OnDiskWriter(Schema schema, DataOutputStream output) {
            super(schema, output);
            this.out = output;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
