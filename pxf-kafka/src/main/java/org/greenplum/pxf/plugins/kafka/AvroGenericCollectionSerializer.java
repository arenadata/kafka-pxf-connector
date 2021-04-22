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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import org.greenplum.pxf.plugins.kafka.codec.LogicalTypeRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class AvroGenericCollectionSerializer implements Serializer<List<GenericRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericCollectionSerializer.class);

    static {
        LogicalTypeRegistrator.registration();
    }

    private final Schema schema;

    public AvroGenericCollectionSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, List<GenericRecord> records) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            for (GenericRecord record : records) {
                try {
                    dataFileWriter.append(record);
                } catch (Exception e) {
                    String message = String.format("Can't serialize record %s by schema %s", record, schema);
                    LOG.error(message, e);
                    throw new RuntimeException(message, e);
                }
            }
            dataFileWriter.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            LOG.error("Serialization failed!", e);
            throw new RuntimeException(e);
        }
    }
}
