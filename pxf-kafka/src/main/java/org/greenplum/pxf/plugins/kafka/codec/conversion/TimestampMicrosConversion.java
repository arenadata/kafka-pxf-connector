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
package org.greenplum.pxf.plugins.kafka.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.greenplum.pxf.plugins.kafka.codec.type.TimestampMicrosLogicalType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class TimestampMicrosConversion extends Conversion<String> {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .appendLiteral(" ")
                    .append(DateTimeFormatter.ofPattern("HH:mm:ss"))
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter();

    private TimestampMicrosConversion() {
        super();
    }

    public static TimestampMicrosConversion getInstance() {
        return TimestampConversionHolder.INSTANCE;
    }

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }

    @Override
    public String getLogicalTypeName() {
        return TimestampMicrosLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return TimestampMicrosLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Long toLong(String value, Schema schema, LogicalType type) {
        Instant instant = LocalDateTime.parse(value, TIMESTAMP_FORMATTER)
                .atOffset(ZoneOffset.UTC).toInstant();
        return instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND);
    }

    private static class TimestampConversionHolder {
        private static final TimestampMicrosConversion INSTANCE = new TimestampMicrosConversion();
    }
}
