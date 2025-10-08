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

import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/** Batch-only CSV parser that converts CSV lines into Flink Rows using an inferred schema. */
public class CsvRowParser {
    private final CsvSchema schema;
    private final char delimiter;
    private final CsvParser parser;
    private final boolean ignoreParseErrors;

    public CsvRowParser(CsvSchema schema, char delimiter) {
        this(schema, delimiter, false);
    }

    public CsvRowParser(CsvSchema schema, char delimiter, boolean ignoreParseErrors) {
        this.schema = schema;
        this.delimiter = delimiter;
        this.ignoreParseErrors = ignoreParseErrors;
        this.parser = new CsvParser(delimiter, true, false, true);
    }

    /** Parse a single CSV line into a Row - for batch processing. */
    public Row parseLine(String line) {
        try {
            String[] fields = parser.parseSingleLine(line);
            Row row = new Row(schema.getColumns().size());

            for (int i = 0; i < schema.getColumns().size(); i++) {
                CsvColumn column = schema.getColumns().get(i);
                String fieldValue = (i < fields.length) ? fields[i] : "";
                row.setField(i, convertToType(fieldValue, column.getInferredType()));
            }
            return row;
        } catch (Exception e) {
            if (ignoreParseErrors) {
                return null;
            } else {
                throw new RuntimeException("Failed to parse CSV line: " + line, e);
            }
        }
    }

    /** Parse multiple lines - for batch file processing. */
    public List<Row> parseLines(List<String> lines) {
        List<Row> rows = new ArrayList<>();
        for (String line : lines) {
            Row row = parseLine(line);
            if (row != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    private Object convertToType(String value, String type) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        try {
            switch (type) {
                case "INT":
                    return Integer.parseInt(value);
                case "DOUBLE":
                    return Double.parseDouble(value);
                case "BOOLEAN":
                    return parseBoolean(value);
                case "DATE":
                    return parseDate(value);
                case "TIME":
                    return parseTime(value);
                case "TIMESTAMP":
                    return parseTimestamp(value);
                case "STRING":
                default:
                    return value;
            }
        } catch (Exception e) {
            if (ignoreParseErrors) {
                return value;
            } else {
                throw new RuntimeException(
                        "Failed to parse value '" + value + "' as type " + type, e);
            }
        }
    }

    private Boolean parseBoolean(String value) {
        String lower = value.toLowerCase();
        return "true".equals(lower)
                || "t".equals(lower)
                || "yes".equals(lower)
                || "1".equals(lower);
    }

    private Date parseDate(String value) {
        try {
            return Date.valueOf(value);
        } catch (Exception e) {
            for (DateTimeFormatter formatter : CsvColumn.DATE_FORMATTERS) {
                try {
                    LocalDate localDate = LocalDate.parse(value, formatter);
                    return Date.valueOf(localDate);
                } catch (Exception ignored) {
                    // Try next formatter
                }
            }
            throw new IllegalArgumentException("Invalid date value: " + value);
        }
    }

    private Time parseTime(String value) {
        try {
            return Time.valueOf(value);
        } catch (Exception e) {
            for (DateTimeFormatter formatter : CsvColumn.TIME_FORMATTERS) {
                try {
                    LocalTime localTime = LocalTime.parse(value, formatter);
                    return Time.valueOf(localTime);
                } catch (Exception ignored) {
                    // Try next formatter
                }
            }
            throw new IllegalArgumentException("Invalid time value: " + value);
        }
    }

    private Timestamp parseTimestamp(String value) {
        try {
            String normalizedValue = value.replace('T', ' ');
            return Timestamp.valueOf(normalizedValue);
        } catch (Exception e) {
            for (DateTimeFormatter formatter : CsvColumn.TIMESTAMP_FORMATTERS) {
                try {
                    LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(localDateTime);
                } catch (Exception ignored) {
                    // Try next formatter
                }
            }
            throw new IllegalArgumentException("Invalid timestamp value: " + value);
        }
    }

    public CsvSchema getSchema() {
        return schema;
    }

    public char getDelimiter() {
        return delimiter;
    }
}
