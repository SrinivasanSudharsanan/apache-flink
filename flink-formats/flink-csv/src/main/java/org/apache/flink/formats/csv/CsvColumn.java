package org.apache.flink.formats.csv;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class CsvColumn {
    private final String name;
    private boolean seenInteger = true;
    private boolean seenDouble = true;
    private boolean seenBoolean = true;
    private boolean seenDate = true;
    private boolean seenTime = true;
    private boolean seenTimestamp = true;
    private int nonNullCount = 0;

    // Common date/time patterns
    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
    private static final Pattern TIME_PATTERN =
            Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?$");
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile(
                    "^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?([Z]|[+-]\\d{2}:?\\d{2})?$");

    // Common formatters
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
        DateTimeFormatter.ISO_LOCAL_DATE,
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy"),
        DateTimeFormatter.ofPattern("dd/MM/yyyy"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd")
    };

    private static final DateTimeFormatter[] TIME_FORMATTERS = {
        DateTimeFormatter.ISO_LOCAL_TIME,
        DateTimeFormatter.ofPattern("HH:mm:ss"),
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("HH:mm")
    };

    private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    };

    public CsvColumn(String name) {
        this.name = name;
    }

    public void analyzeValue(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }

        nonNullCount++;

        // Check if it's a boolean
        if (seenBoolean) {
            String lower = value.toLowerCase();
            if (!("true".equals(lower)
                    || "false".equals(lower)
                    || "t".equals(lower)
                    || "f".equals(lower)
                    || "yes".equals(lower)
                    || "no".equals(lower)
                    || "1".equals(lower)
                    || "0".equals(lower))) {
                seenBoolean = false;
            }
        }

        // Check if it's an integer
        if (seenInteger) {
            try {
                Long.parseLong(value);
            } catch (NumberFormatException e) {
                seenInteger = false;
            }
        }

        // Check if it's a double
        if (seenDouble) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                seenDouble = false;
            }
        }

        // Check if it's a date
        if (seenDate) {
            seenDate = isDate(value);
        }

        // Check if it's a time
        if (seenTime) {
            seenTime = isTime(value);
        }

        // Check if it's a timestamp
        if (seenTimestamp) {
            seenTimestamp = isTimestamp(value);
        }
    }

    private boolean isDate(String value) {
        // Quick pattern check first
        if (!DATE_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Try parsing with various formatters
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                LocalDate.parse(value, formatter);
                return true;
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }
        return false;
    }

    private boolean isTime(String value) {
        // Quick pattern check first
        if (!TIME_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Try parsing with various formatters
        for (DateTimeFormatter formatter : TIME_FORMATTERS) {
            try {
                LocalTime.parse(value, formatter);
                return true;
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }
        return false;
    }

    private boolean isTimestamp(String value) {
        // Quick pattern check first
        if (!TIMESTAMP_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Try parsing with various formatters
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                LocalDateTime.parse(value, formatter);
                return true;
            } catch (DateTimeParseException e) {
                // Try next formatter
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }

    public String getInferredType() {
        if (nonNullCount == 0) {
            return "STRING";
        }

        // Type precedence: DATE/TIME/TIMESTAMP -> BOOLEAN -> INTEGER -> DOUBLE -> STRING
        if (seenTimestamp) {
            return "TIMESTAMP";
        }
        if (seenDate) {
            return "DATE";
        }
        if (seenTime) {
            return "TIME";
        }
        if (seenBoolean) {
            return "BOOLEAN";
        }
        if (seenInteger) {
            return "INT";
        }
        if (seenDouble) {
            return "DOUBLE";
        }
        return "STRING";
    }

    // Getters for debugging

    public boolean hasSeenInteger() {
        return seenInteger;
    }

    public boolean hasSeenDouble() {
        return seenDouble;
    }

    public boolean hasSeenBoolean() {
        return seenBoolean;
    }

    public boolean hasSeenDate() {
        return seenDate;
    }

    public boolean hasSeenTime() {
        return seenTime;
    }

    public boolean hasSeenTimestamp() {
        return seenTimestamp;
    }

    public int getNonNullCount() {
        return nonNullCount;
    }
}
