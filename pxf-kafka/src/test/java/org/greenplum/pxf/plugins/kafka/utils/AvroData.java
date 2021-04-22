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
package org.greenplum.pxf.plugins.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AvroData {

    final Schema schema;
    final List<GenericRecord> records;

    private AvroData(Schema schema, List<GenericRecord> records) {
        this.schema = schema;
        this.records = records;
    }

    public static AvroData convert(byte[] bytes) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader);
        return new AvroData(dataFileReader.getSchema(),
                StreamSupport.stream(dataFileReader.spliterator(), false).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(schema.toString());
        records.forEach(r -> builder.append('\n').append(r.toString()));
        return builder.toString();
    }

    public Schema getSchema() {
        return schema;
    }

    public List<GenericRecord> getRecords() {
        return records;
    }
}
