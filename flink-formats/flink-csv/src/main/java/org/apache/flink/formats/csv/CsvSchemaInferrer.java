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

                    // FIXED: Generate column names for no-header files
                    if (!hasHeader) {
                        for (int i = 0; i < columnsCount; i++) {
                            columnNames.add("col_" + i);
                        }
                        System.out.println("DEBUG: Generated " + columnsCount + " column names for no-header file");
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

    private static String inferColumnType(List<String> columnValues) {
        if (columnValues.isEmpty()) {
            return "STRING";
        }

        int integerCount = 0;
        int doubleCount = 0;
        int booleanCount = 0;
        int timestampCount = 0;
        int dateCount = 0;
        int timeCount = 0;
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
            else if (value.matches("-?\\d*\\.\\d+([eE][-+]?\\d+)?")) {
                doubleCount++;
            }
            // Check if boolean (more patterns)
            else if (value.matches("(?i)true|false|yes|no|1|0")) {
                booleanCount++;
            }
            // Check if timestamp (ISO format)
            else if (isTimestamp(value)) {
                timestampCount++;
            }
            // Check if date (date only)
            else if (isDate(value)) {
                dateCount++;
            }
            // Check if time (time only)
            else if (isTime(value)) {
                timeCount++;
            }
        }

        if (total == 0) {
            return "STRING";
        }

        // Use threshold-based detection instead of requiring 100% consistency
        double threshold = 0.8; // 80% of values must match the type

        if ((double) integerCount / total >= threshold) {
            return "INT";
        }
        if ((double) doubleCount / total >= threshold) {
            return "DOUBLE";
        }
        if ((double) booleanCount / total >= threshold) {
            return "BOOLEAN";
        }
        if ((double) timestampCount / total >= threshold) {
            return "TIMESTAMP";
        }
        if ((double) dateCount / total >= threshold) {
            return "DATE";
        }
        if ((double) timeCount / total >= threshold) {
            return "TIME";
        }

        return "STRING";
    }

    /** Enhanced timestamp detection. */
    private static boolean isTimestamp(String value) {
        // Remove surrounding quotes if present
        String cleaned = value.trim().replaceAll("^\"|\"$", "");

        // ISO timestamp: 2023-01-15 10:30:00 or 2023-01-15T10:30:00
        if (cleaned.matches("\\d{4}-\\d{2}-\\d{2}[T\\s]\\d{2}:\\d{2}:\\d{2}")) {
            return true;
        }

        // Common timestamp formats
        if (cleaned.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
            return true;
        }
        if (cleaned.matches("\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}")) {
            return true;
        }

        return false;
    }

    /** Date detection (date part only). */
    private static boolean isDate(String value) {
        String cleaned = value.trim().replaceAll("^\"|\"$", "");

        // ISO date: 2023-01-15
        if (cleaned.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return true;
        }

        // Common date formats
        if (cleaned.matches("\\d{2}/\\d{2}/\\d{4}")) {
            return true;
        }

        return false;
    }

    /** Time detection (time part only). */
    private static boolean isTime(String value) {
        String cleaned = value.trim().replaceAll("^\"|\"$", "");

        // ISO time: 10:30:00
        if (cleaned.matches("\\d{2}:\\d{2}:\\d{2}")) {
            return true;
        }

        // Simple time: 10:30
        if (cleaned.matches("\\d{2}:\\d{2}")) {
            return true;
        }

        return false;
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
            boolean hasHeader = args.length > 1 ? Boolean.parseBoolean(args[1]) : true;
            int sampleSize = args.length > 2 ? Integer.parseInt(args[2]) : 100;
            System.out.println("DEBUG: filePath=" + args[0] + ", hasHeader=" + hasHeader + ", sampleSize=" + sampleSize);
            SchemaResult result = inferSchema(args[0], hasHeader, sampleSize);
            System.out.println(result);
        } else {
            System.out.println("Usage: java CsvSchemaInferrer <csv-file> [hasHeader] [sampleSize]");
            System.out.println("  hasHeader: true/false (default: true)");
            System.out.println("  sampleSize: number of rows to sample (default: 100)");
        }
    }
}