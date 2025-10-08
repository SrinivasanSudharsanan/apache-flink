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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** CSV schema inferrer with multi-line support. */
public class CsvSchemaInferrer {
    private final boolean hasHeader;
    private final int sampleSize;
    private final CsvParser parser;

    public CsvSchemaInferrer(
            boolean hasHeader, int sampleSize, char delimiter, boolean allowMultiLine) {
        this.hasHeader = hasHeader;
        this.sampleSize = sampleSize;
        this.parser = new CsvParser(delimiter, true, false, allowMultiLine);
    }

    /** Infer schema from a CSV file. */
    public CsvSchema inferSchema(String filePath) throws IOException {
        List<String[]> samples = new ArrayList<>();
        List<String> header = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int linesRead = 0;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null && linesRead < sampleSize) {
                if (line.isEmpty() && !parser.isInMultiLineState()) {
                    continue;
                }

                String[] fields = parser.parseLine(line, false);
                if (fields == null) {
                    continue;
                }

                if (isFirstLine && hasHeader) {
                    header = List.of(fields);
                    isFirstLine = false;
                } else {
                    samples.add(fields);
                    linesRead++;
                }
            }

            String[] finalFields = parser.parseLine(null, true);
            if (finalFields != null && finalFields.length > 0) {
                boolean allEmpty = true;
                for (String f : finalFields) {
                    if (f != null && !f.isEmpty()) {
                        allEmpty = false;
                        break;
                    }
                }
                if (!allEmpty && samples.size() < sampleSize) {
                    samples.add(finalFields);
                }
            }
        }

        if (samples.isEmpty()) {
            throw new IllegalArgumentException("No data samples found to infer schema");
        }

        return analyzeSchema(samples, header);
    }

    private CsvSchema analyzeSchema(List<String[]> samples, List<String> header) {
        int columnCount = samples.get(0).length;
        CsvColumn[] columns = new CsvColumn[columnCount];

        for (int i = 0; i < columnCount; i++) {
            String columnName =
                    (header != null && i < header.size()) ? header.get(i) : "field_" + i;
            columns[i] = new CsvColumn(columnName);
        }

        for (String[] sample : samples) {
            if (sample.length != columnCount) {
                continue;
            }
            for (int i = 0; i < columnCount; i++) {
                columns[i].analyzeValue(sample[i]);
            }
        }

        CsvSchema schema = new CsvSchema();
        for (CsvColumn column : columns) {
            schema.addColumn(column);
        }
        return schema;
    }
}
