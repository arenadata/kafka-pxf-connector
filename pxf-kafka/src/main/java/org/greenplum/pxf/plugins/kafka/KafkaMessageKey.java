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
import org.apache.avro.reflect.ReflectData;

import java.util.stream.Collectors;

public class KafkaMessageKey {

    public static final Schema SCHEMA = Schema.createRecord("key", null, null, false);

    static {
        SCHEMA.setFields(ReflectData.get().getSchema(KafkaMessageKey.class).getFields().stream()
                .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).collect(Collectors.toList()));
    }

    private final String tableName;
    private final int streamNumber;
    private final int streamTotal;
    private int chunkNumber;
    private boolean isLastChunk;

    private KafkaMessageKey(String tableName, int streamNumber, int streamTotal) {
        this.tableName = tableName;
        this.streamNumber = streamNumber;
        this.streamTotal = streamTotal;
    }

    public static KafkaMessageKey template(String tableName, int streamNumber, int streamTotal) {
        return new KafkaMessageKey(tableName, streamNumber, streamTotal);
    }

    public KafkaMessageKey withChunkInfo(int chunkNumber, boolean isLastChunk) {
        this.chunkNumber = chunkNumber;
        this.isLastChunk = isLastChunk;
        return this;
    }

    @SuppressWarnings("unused")
    public String getTableName() {
        return tableName;
    }

    @SuppressWarnings("unused")
    public int getStreamNumber() {
        return streamNumber;
    }

    @SuppressWarnings("unused")
    public int getStreamTotal() {
        return streamTotal;
    }

    @SuppressWarnings("unused")
    public int getChunkNumber() {
        return chunkNumber;
    }

    @SuppressWarnings("unused")
    public boolean isLastChunk() {
        return isLastChunk;
    }
}
