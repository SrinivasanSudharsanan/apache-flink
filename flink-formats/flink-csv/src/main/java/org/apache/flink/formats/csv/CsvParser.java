package org.apache.flink.formats.csv;

import java.util.ArrayList;
import java.util.List;

/**
 * Enhanced CSV parser with support for multi-line fields. custom delimiters, and performance
 * optimizations.
 */
public class CsvParser {
    private final char delimiter;
    private final boolean handleQuotes;
    private final boolean trimWhitespace;
    private final boolean allowMultiLine;

    private StringBuilder currentField = new StringBuilder();
    private List<String> currentFields = new ArrayList<>();
    private boolean inQuotes = false;
    private boolean escapeNext = false;

    // Statistics
    private int linesProcessed = 0;
    private int fieldsProcessed = 0;

    public CsvParser() {
        this(',', true, false, true);
    }

    public CsvParser(
            char delimiter, boolean handleQuotes, boolean trimWhitespace, boolean allowMultiLine) {
        this.delimiter = delimiter;
        this.handleQuotes = handleQuotes;
        this.trimWhitespace = trimWhitespace;
        this.allowMultiLine = allowMultiLine;
    }

    /** Parse a CSV line with proper handling for multi-line fields and trailing empty columns. */
    public String[] parseLine(String line, boolean isEndOfStream) {
        linesProcessed++;

        if (line == null) {
            return completeRecord(isEndOfStream);
        }

        System.out.println("DEBUG PARSER: Parsing line: [" + line + "]");

        // If we're continuing a multi-line field, start with the existing state
        boolean continueFromPending = inQuotes && allowMultiLine;

        if (!continueFromPending) {
            currentField.setLength(0);
            currentFields.clear();
            inQuotes = false;
            escapeNext = false;
        } else {
            // When continuing a multi-line field, add a newline to preserve the line break
            // but only if we're not at the beginning of the field
            if (currentField.length() > 0) {
                currentField.append('\n');
            }
        }

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (escapeNext) {
                currentField.append(c);
                escapeNext = false;
            } else if (c == '\\' && handleQuotes) {
                escapeNext = true;
            } else if (c == '"' && handleQuotes) {
                inQuotes = !inQuotes;
            } else if (c == delimiter && (!handleQuotes || !inQuotes)) {
                addCurrentField();
            } else {
                currentField.append(c);
            }
        }

        // Handle end of line
        if (!allowMultiLine || isEndOfStream) {
            // Force completion at end of line or stream
            return completeRecord(true);
        } else if (inQuotes) {
            // Field continues to next line - don't add newline here, we'll add it at the start of
            // next line
            System.out.println("DEBUG PARSER: Multi-line field continues to next line");
            return null;
        } else {
            // Complete record
            return completeRecord(true);
        }
    }

    /** Reset parser state (useful for new files or error recovery). */
    public void reset() {
        currentField.setLength(0);
        currentFields.clear();
        inQuotes = false;
        escapeNext = false;
    }

    /** Parse a single line without multi-line support (simpler interface). */
    public String[] parseSingleLine(String line) {
        return parseLine(line, true);
    }

    private void addCurrentField() {
        String field = currentField.toString();
        if (trimWhitespace) {
            field = field.trim();
        }

        // CRITICAL FIX: Always add the field, even if empty, to preserve column structure
        // This ensures trailing commas create empty columns as expected in CSV
        System.out.println("DEBUG PARSER: Adding field: [" + field + "]");
        currentFields.add(field);
        fieldsProcessed++;

        currentField.setLength(0);
    }

    private String[] completeRecord(boolean forceComplete) {
        if (forceComplete || !inQuotes) {
            // Add the last field (might be empty - that's OK for trailing columns)
            addCurrentField();
            String[] result = currentFields.toArray(new String[0]);
            System.out.println("DEBUG PARSER: Final fields count: " + result.length);

            // Reset for next record (but preserve multi-line state if not forced)
            if (forceComplete) {
                currentField.setLength(0);
                currentFields.clear();
                if (!allowMultiLine) {
                    inQuotes = false;
                    escapeNext = false;
                }
            }

            return result;
        }
        return null;
    }

    // Getters for statistics and state

    public int getLinesProcessed() {
        return linesProcessed;
    }

    public int getFieldsProcessed() {
        return fieldsProcessed;
    }

    public boolean isInMultiLineState() {
        return inQuotes && allowMultiLine;
    }

    /** Static utility method for simple CSV parsing. */
    public static String[] parseCsvLine(String line) {
        return parseCsvLine(line, ',', true, false);
    }

    public static String[] parseCsvLine(
            String line, char delimiter, boolean handleQuotes, boolean trimWhitespace) {
        CsvParser parser = new CsvParser(delimiter, handleQuotes, trimWhitespace, false);
        return parser.parseSingleLine(line);
    }
}
