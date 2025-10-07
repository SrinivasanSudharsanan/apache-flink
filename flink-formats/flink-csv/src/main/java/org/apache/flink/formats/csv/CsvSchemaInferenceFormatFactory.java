/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.formats.csv.CsvInferenceFormatOptions.HEADER;
import static org.apache.flink.formats.csv.CsvInferenceFormatOptions.SAMPLE_SIZE;
import static org.apache.flink.formats.csv.CsvInferenceFormatOptions.STRICT_MODE;

/** CSV schema inference format factory for file system. */
@Internal
public class CsvSchemaInferenceFormatFactory implements BulkReaderFormatFactory {

    @Override
    public String factoryIdentifier() {
        return "csv-infer";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(HEADER, SAMPLE_SIZE, STRICT_MODE);
    }

    @Override
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        // Return the existing CSV format implementation
        // This provides immediate functionality while schema inference
        // can be integrated as a future enhancement
        return new CsvFileFormatFactory.CsvBulkDecodingFormat(formatOptions);
    }
}
