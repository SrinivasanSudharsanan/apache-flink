/*
 * Licensed to the Apache Software Foundation (ASF) under one.
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information.
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance.
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software.
 * distributed under the License is distributed on an "AS IS" BASIS,.
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and.
 * limitations under the License.
 */

package org.apache.flink.formats.csv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Core schema inference logic for CSV files. */
public class CsvSchemaInferrer {

    /**
     * Infers schema from CSV file by reading header and sampling data rows. Returns column names
     * and inferred types.
     */
    public static SchemaResult inferSchema(String filePath, boolean hasHeader, int sampleSize) {
        System.out.println("Inferring schema for: " + filePath);

        List<String> columnNames = new ArrayList<>();
        List<List<String>> columnSamples = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 0;
            int dataRowCount = 0;
            int columnsCount = -1;

            // Read and process each line
            while ((line = reader.readLine()) != null && dataRowCount < sampleSize) {
                String[] fields = parseCsvLine(line);

                if (columnsCount == -1) {
                    // First row - initialize columns
                    columnsCount = fields.length;
                    for (int i = 0; i < columnsCount; i++) {
                        columnSamples.add(new ArrayList<>());
                    }
                }

                if (lineNumber == 0 && hasHeader) {
                    // First row is header
                    for (String field : fields) {
                        columnNames.add(field.trim());
                    }
                } else {
                    // Data row - sample values for type inference
                    for (int i = 0; i < Math.min(fields.length, columnsCount); i++) {
                        if (i < columnSamples.size()) {
                            columnSamples.get(i).add(fields[i].trim());
                        }
                    }

                    // Generate column names if no header
                    if (lineNumber == 0 && !hasHeader) {
                        for (int i = 0; i < columnsCount; i++) {
                            columnNames.add("col_" + i);
                        }
                    }

                    dataRowCount++;
                }
                lineNumber++;
            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return new SchemaResult(List.of("error"), List.of("STRING"));
        }

        // Infer types for each column
        List<String> columnTypes = new ArrayList<>();
        for (List<String> columnValues : columnSamples) {
            columnTypes.add(inferColumnType(columnValues));
        }

        return new SchemaResult(columnNames, columnTypes);
    }

    /** Simple CSV line parser - split by comma, handle basic quotes. */
    private static String[] parseCsvLine(String line) {
        // For now, use simple split - we can enhance later
        return line.split(",");
    }

    /** Advanced type inference from sample values. */
    private static String inferColumnType(List<String> columnValues) {
        if (columnValues.isEmpty()) {
            return "STRING";
        }

        int integerCount = 0;
        int doubleCount = 0;
        int booleanCount = 0;
        int total = 0;

        for (String value : columnValues) {
            if (value == null || value.isEmpty() || value.equals("null")) {
                continue;
            }
            total++;

            // Check if integer
            if (value.matches("-?\\d+")) {
                integerCount++;
            }
            // Check if double (but not integer)
            else if (value.matches("-?\\d*\\.\\d+")) {
                doubleCount++;
            }
            // Check if boolean
            else if (value.matches("(?i)true|false")) {
                booleanCount++;
            }
        }

        if (total == 0) {
            return "STRING";
        }
        // // Use majority voting
        if (integerCount == total) {
            return "INT";
        }
        if (doubleCount == total) {
            return "DOUBLE";
        }
        if (booleanCount == total) {
            return "BOOLEAN";
        }
        return "STRING";
    }

    /** Result container for inferred schema. */
    public static class SchemaResult {
        private final List<String> columnNames;
        private final List<String> columnTypes;

        public SchemaResult(List<String> columnNames, List<String> columnTypes) {
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public List<String> getColumnTypes() {
            return columnTypes;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Inferred Schema:\n");
            int minSize = Math.min(columnNames.size(), columnTypes.size());
            for (int i = 0; i < minSize; i++) {
                sb.append("  ")
                        .append(columnNames.get(i))
                        .append(" : ")
                        .append(columnTypes.get(i))
                        .append("\n");
            }
            return sb.toString();
        }
    }

    /** Test method. */
    public static void main(String[] args) {
        if (args.length > 0) {
            SchemaResult result = inferSchema(args[0], true, 100);
            System.out.println(result);
        } else {
            System.out.println("Usage: java CsvSchemaInferrer <csv-file>");
        }
    }
}
