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
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class AvroReflectBeanSerializer<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroReflectBeanSerializer.class);

    private final Schema schema;

    public AvroReflectBeanSerializer(Schema schema) {
        this.schema = schema;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        @SuppressWarnings("unchecked")
        DatumWriter<T> datumWriter = ReflectData.get().createDatumWriter(schema);
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(data);
            dataFileWriter.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            String message = String.format("Can't serialize record %s by schema %s", data, schema);
            LOG.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
