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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** CSV schema inferrer with multi-line support and robust error handling. */
public class CsvSchemaInferrer {
    private final boolean hasHeader;
    private final int sampleSize;
    private final CsvParser parser;
    private final boolean strictMode;
    private final String filePath;

    public CsvSchemaInferrer(
            boolean hasHeader, int sampleSize, char delimiter, boolean allowMultiLine) {
        this(hasHeader, sampleSize, delimiter, allowMultiLine, false, null);
    }

    public CsvSchemaInferrer(
            boolean hasHeader,
            int sampleSize,
            char delimiter,
            boolean allowMultiLine,
            boolean strictMode,
            String filePath) {
        this.hasHeader = hasHeader;
        this.sampleSize = sampleSize;
        this.strictMode = strictMode;
        this.filePath = filePath;
        this.parser = new CsvParser(delimiter, true, false, allowMultiLine);
    }

    /** Infer schema from a CSV file with comprehensive error handling. */
    public CsvSchema inferSchema(String filePath) throws IOException {
        List<String[]> samples = new ArrayList<>();
        List<String> header = null;
        int lineNumber = 0;
        int linesRead = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null && linesRead < sampleSize) {
                lineNumber++;

                // Skip empty lines (unless in multi-line state)
                if (line.trim().isEmpty() && !parser.isInMultiLineState()) {
                    continue;
                }

                try {
                    String[] fields = parser.parseLine(line, false);

                    if (fields == null) {
                        // Multi-line field continues
                        continue;
                    }

                    if (isFirstLine && hasHeader) {
                        header = validateHeader(fields, filePath, lineNumber);
                        isFirstLine = false;
                    } else {
                        samples.add(validateSample(fields, filePath, lineNumber, line));
                        linesRead++;
                    }

                } catch (Exception e) {
                    if (strictMode) {
                        throw new CsvParseInferenceException(
                                filePath, lineNumber, line, "Failed to parse CSV line", e);
                    }
                    // In non-strict mode, skip malformed lines but log warning
                    System.err.printf(
                            "Warning: Skipping malformed line %d in %s: %s%n",
                            lineNumber, filePath, e.getMessage());
                }
            }

            // Process any remaining multi-line field
            try {
                String[] finalFields = parser.parseLine(null, true);
                if (finalFields != null && finalFields.length > 0 && samples.size() < sampleSize) {
                    boolean allEmpty = true;
                    for (String field : finalFields) {
                        if (field != null && !field.trim().isEmpty()) {
                            allEmpty = false;
                            break;
                        }
                    }
                    if (!allEmpty) {
                        samples.add(
                                validateSample(
                                        finalFields, filePath, lineNumber + 1, "end-of-file"));
                    }
                }
            } catch (Exception e) {
                if (strictMode) {
                    throw new CsvParseInferenceException(
                            filePath,
                            lineNumber + 1,
                            "end-of-file",
                            "Failed to parse final multi-line field",
                            e);
                }
            }

        } catch (IOException e) {
            throw new IOException("Failed to read CSV file for schema inference: " + filePath, e);
        }

        // Validate that we got enough samples
        if (samples.isEmpty()) {
            throw new CsvSchemaInferenceException(
                    "No valid data samples found to infer schema from file: "
                            + filePath
                            + ". File may be empty, contain only headers, or have severe formatting issues.");
        }

        if (samples.size() < Math.min(5, sampleSize)) {
            System.err.printf(
                    "Warning: Only %d samples found for schema inference in %s%n",
                    samples.size(), filePath);
        }

        return analyzeSchema(samples, header, filePath);
    }

    private List<String> validateHeader(String[] headerFields, String filePath, int lineNumber) {
        if (headerFields.length == 0) {
            throw new CsvParseInferenceException(
                    filePath, lineNumber, Arrays.toString(headerFields), "Header line is empty");
        }

        // Check for duplicate header names
        Set<String> uniqueNames = new HashSet<>();
        for (String field : headerFields) {
            if (field != null && !field.trim().isEmpty()) {
                if (!uniqueNames.add(field.trim())) {
                    throw new CsvParseInferenceException(
                            filePath,
                            lineNumber,
                            Arrays.toString(headerFields),
                            "Duplicate header name found: " + field);
                }
            }
        }

        return List.of(headerFields);
    }

    private String[] validateSample(
            String[] fields, String filePath, int lineNumber, String lineContent) {
        if (fields.length == 0) {
            throw new CsvParseInferenceException(
                    filePath, lineNumber, lineContent, "Sample line has no fields after parsing");
        }
        return fields;
    }

    private CsvSchema analyzeSchema(List<String[]> samples, List<String> header, String filePath) {
        int columnCount = samples.get(0).length;
        CsvColumn[] columns = new CsvColumn[columnCount];

        // Initialize columns
        for (int i = 0; i < columnCount; i++) {
            String columnName =
                    (header != null && i < header.size()) ? header.get(i) : "field_" + (i + 1);
            columns[i] = new CsvColumn(columnName, strictMode);
        }

        // Analyze each sample
        for (int sampleIndex = 0; sampleIndex < samples.size(); sampleIndex++) {
            String[] sample = samples.get(sampleIndex);

            if (sample.length != columnCount) {
                if (strictMode) {
                    throw new CsvParseInferenceException(
                            filePath,
                            sampleIndex + (hasHeader ? 2 : 1),
                            Arrays.toString(sample),
                            "Column count mismatch. Expected: "
                                    + columnCount
                                    + ", Got: "
                                    + sample.length);
                }
                // In non-strict mode, skip rows with column count mismatches
                continue;
            }

            for (int i = 0; i < columnCount; i++) {
                try {
                    columns[i].analyzeValue(sample[i]);
                } catch (CsvTypeInferenceException e) {
                    if (strictMode) {
                        throw e;
                    }
                    // In non-strict mode, log and continue
                    System.err.printf(
                            "Warning: Type inference issue in %s: %s%n", filePath, e.getMessage());
                }
            }
        }

        CsvSchema schema = new CsvSchema();
        for (CsvColumn column : columns) {
            schema.addColumn(column);
        }

        // Validate that we have at least some non-null data
        boolean allColumnsEmpty = true;
        for (CsvColumn column : columns) {
            if (column.getNonNullCount() > 0) {
                allColumnsEmpty = false;
                break;
            }
        }

        if (allColumnsEmpty) {
            throw new CsvSchemaInferenceException(
                    "All columns contain only null or empty values in file: " + filePath);
        }

        return schema;
    }
}
