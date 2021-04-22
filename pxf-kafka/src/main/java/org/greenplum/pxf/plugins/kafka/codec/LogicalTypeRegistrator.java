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
package org.greenplum.pxf.plugins.kafka.codec;

import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.greenplum.pxf.plugins.kafka.codec.conversion.DateConversion;
import org.greenplum.pxf.plugins.kafka.codec.conversion.TimeMicrosConversion;
import org.greenplum.pxf.plugins.kafka.codec.conversion.TimestampMicrosConversion;
import org.greenplum.pxf.plugins.kafka.codec.type.DateLogicalType;
import org.greenplum.pxf.plugins.kafka.codec.type.TimeMicrosLogicalType;
import org.greenplum.pxf.plugins.kafka.codec.type.TimestampMicrosLogicalType;

public class LogicalTypeRegistrator {
    public static void registration() {
        GenericData.get().addLogicalTypeConversion(DateConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(TimeMicrosConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(TimestampMicrosConversion.getInstance());

        SpecificData.get().addLogicalTypeConversion(DateConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(TimeMicrosConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(TimestampMicrosConversion.getInstance());

        LogicalTypes.register(DateLogicalType.INSTANCE.getName(), schema -> DateLogicalType.INSTANCE);
        LogicalTypes.register(TimeMicrosLogicalType.INSTANCE.getName(), schema -> TimestampMicrosLogicalType.INSTANCE);
        LogicalTypes.register(TimestampMicrosLogicalType.INSTANCE.getName(), schema -> TimestampMicrosLogicalType.INSTANCE);
    }
}
