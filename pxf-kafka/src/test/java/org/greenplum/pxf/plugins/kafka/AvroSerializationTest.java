/**
 * Copyright © 2021 kafka-pxf-connector
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.greenplum.pxf.plugins.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.greenplum.pxf.plugins.kafka.utils.AvroData;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AvroSerializationTest {

    private static final String ID_NAME_SCHEMA_JSON = "{\"name\":\"row\",\"type\":\"record\"," +
            "    \"fields\":[" +
            "      {\"name\":\"id\",\"type\": [\"int\",\"null\"]}," +
            "      {\"name\": \"name\",\"type\": [\"string\",\"null\"]}" +
            "    ]}";

    @Test
    public void testKeySerialization() throws IOException {
        KafkaMessageKey key = KafkaMessageKey.template("test", 0, 1)
                .withChunkInfo(0, true);
        Serializer<KafkaMessageKey> serializer = new AvroReflectBeanSerializer<>(KafkaMessageKey.SCHEMA);
        final byte[] bytes = serializer.serialize("test", key);
        final AvroData avro = AvroData.convert(bytes);
        Assert.assertEquals(avro.getSchema().getName(), KafkaMessageKey.SCHEMA.getName());
        Assert.assertEquals(avro.getSchema().getNamespace(), KafkaMessageKey.SCHEMA.getNamespace());
        Assert.assertEquals(avro.getSchema().getFields(), KafkaMessageKey.SCHEMA.getFields());
        Assert.assertEquals(1, avro.getRecords().size());
        Assert.assertEquals("test", avro.getRecords().get(0).get("tableName").toString());
    }

    @Test
    public void testDataSerialization() throws IOException {
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(ID_NAME_SCHEMA_JSON);
        GenericRecord genericRecord;
        List<GenericRecord> list = new ArrayList<>();
        genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, 1);
        genericRecord.put(1, "qwe");
        list.add(genericRecord);
        genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 2);
        genericRecord.put("name", "asd");
        list.add(genericRecord);
        Serializer<List<GenericRecord>> serializer = new AvroGenericCollectionSerializer(schema);
        final byte[] bytes = serializer.serialize("test", list);
        final AvroData avro = AvroData.convert(bytes);
        Assert.assertEquals(avro.getSchema().getName(), schema.getName());
        Assert.assertEquals(avro.getSchema().getNamespace(), schema.getNamespace());
        Assert.assertEquals(avro.getSchema().getFields(), schema.getFields());
        Assert.assertEquals(2, avro.getRecords().size());
        Assert.assertEquals(list, avro.getRecords());
    }

    @Test
    public void testSchemaAndEmptyRows() throws IOException {
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(ID_NAME_SCHEMA_JSON);
        Assert.assertEquals("row", schema.getName());
        List<Schema> unionList;
        final List<Schema.Field> fields = new ArrayList<>();
        unionList = new ArrayList<>();
        unionList.add(Schema.create(Schema.Type.INT));
        unionList.add(Schema.create(Schema.Type.NULL));
        fields.add(new Schema.Field("id", Schema.createUnion(unionList), "", null));
        unionList = new ArrayList<>();
        unionList.add(Schema.create(Schema.Type.STRING));
        unionList.add(Schema.create(Schema.Type.NULL));
        fields.add(new Schema.Field("name", Schema.createUnion(unionList), "", null));
        Assert.assertEquals(fields, schema.getFields());
        Serializer<List<GenericRecord>> serializer = new AvroGenericCollectionSerializer(schema);
        final byte[] bytes = serializer.serialize("test", Collections.emptyList());
        final AvroData avro = AvroData.convert(bytes);
        Assert.assertEquals(avro.getSchema().getName(), schema.getName());
        Assert.assertEquals(avro.getSchema().getNamespace(), schema.getNamespace());
        Assert.assertEquals(avro.getSchema().getFields(), schema.getFields());
        Assert.assertEquals(0, avro.getRecords().size());
    }
}
