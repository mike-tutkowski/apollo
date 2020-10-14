package com.datastax.rows;

import com.datastax.metadata.Schema;
import com.datastax.rows.Row;
import com.datastax.serde.IntType;
import com.datastax.serde.TextType;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class RowTest {

    @Test
    public void rowSerializationRoundTrip() throws Throwable {
        Schema schema = new Schema.Builder()
                .addKey("ck1", TextType.instance)
                .addKey("ck2", IntType.instance)
                .addColumn("v", TextType.instance)
                .build();

        Row row = Row.builder(schema)
                .addKey("ck1", "ck001")
                .addKey("ck2", 1)
                .addColumn("v", "v00")
                .row();

        ByteArrayOutputStream os = new ByteArrayOutputStream(row.serializedSize());
        DataOutput output = new DataOutputStream(os);
        row.serialize(output);
        os.close();

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        DataInput input = new DataInputStream(is);
        Row deserialized = Row.deserialize(schema, input);
        Assert.assertEquals(row, deserialized);
        Assert.assertEquals(is.available(), 0);
    }

}
