package org.apache.flink.formats.csv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/** Enhanced CSV schema inferrer with improved parsing capabilities. */
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
        System.out.println(
                "DEBUG: filePath="
                        + filePath
                        + ", hasHeader="
                        + hasHeader
                        + ", sampleSize="
                        + sampleSize);
        System.out.println("Inferring schema for: " + filePath);

        List<String[]> samples = new ArrayList<>();
        List<String> header = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int linesRead = 0;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null && linesRead < sampleSize) {
                // CRITICAL FIX: Only skip completely empty lines when not in multi-line mode
                // This preserves whitespace-only lines that are part of multi-line fields
                if (line.isEmpty() && !parser.isInMultiLineState()) {
                    System.out.println("DEBUG: Skipping completely empty line");
                    continue;
                }

                String[] fields = parser.parseLine(line, false);

                if (fields == null) {
                    // Multi-line field, continue reading
                    System.out.println("DEBUG: Multi-line field continues to next line");
                    continue;
                }

                if (isFirstLine && hasHeader) {
                    header = List.of(fields);
                    isFirstLine = false;
                    System.out.println(
                            "DEBUG: Parsed header with "
                                    + fields.length
                                    + " fields: "
                                    + List.of(fields));
                } else {
                    samples.add(fields);
                    linesRead++;
                    System.out.println(
                            "DEBUG: Parsed " + fields.length + " fields: " + List.of(fields));
                }
            }

            // Handle any pending multi-line record at end of sample - FIXED VERSION
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
                    System.out.println(
                            "DEBUG: Parsed final "
                                    + finalFields.length
                                    + " fields: "
                                    + List.of(finalFields));
                }
            }
        }

        CsvSchema schema = analyzeSchema(samples, header);
        printDetailedAnalysis(schema.getColumns().toArray(new CsvColumn[0]), samples);
        return schema;
    }

    /** Infer schema from CSV content as a string with proper multi-line support. */
    public CsvSchema inferSchemaFromString(String csvContent, boolean hasHeader)
            throws IOException {
        System.out.println(
                "DEBUG: Inferring schema from CSV string, hasHeader="
                        + hasHeader
                        + ", sampleSize="
                        + sampleSize);

        List<String[]> samples = new ArrayList<>();
        List<String> header = null;

        try (BufferedReader reader = new BufferedReader(new StringReader(csvContent))) {
            String line;
            int linesRead = 0;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null && linesRead < sampleSize) {
                // CRITICAL FIX: Only skip completely empty lines when not in multi-line mode
                if (line.isEmpty() && !parser.isInMultiLineState()) {
                    System.out.println("DEBUG: Skipping completely empty line");
                    continue;
                }

                String[] fields = parser.parseLine(line, false);

                if (fields == null) {
                    // Multi-line field, continue reading
                    System.out.println("DEBUG: Multi-line field continues to next line");
                    continue;
                }

                if (isFirstLine && hasHeader) {
                    header = List.of(fields);
                    isFirstLine = false;
                    System.out.println(
                            "DEBUG: Parsed header with "
                                    + fields.length
                                    + " fields: "
                                    + List.of(fields));
                } else {
                    samples.add(fields);
                    linesRead++;
                    System.out.println(
                            "DEBUG: Parsed " + fields.length + " fields: " + List.of(fields));
                }
            }

            // Handle any pending multi-line record at end - FIXED VERSION
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
                    System.out.println(
                            "DEBUG: Parsed final "
                                    + finalFields.length
                                    + " fields: "
                                    + List.of(finalFields));
                }
            }
        }

        CsvSchema schema = analyzeSchema(samples, header);
        printDetailedAnalysis(schema.getColumns().toArray(new CsvColumn[0]), samples);
        return schema;
    }

    /** Infer schema from a list of CSV lines with proper multi-line support. */
    public CsvSchema inferSchemaFromLines(List<String> csvLines, boolean hasHeader) {
        System.out.println(
                "DEBUG: Inferring schema from "
                        + csvLines.size()
                        + " lines, hasHeader="
                        + hasHeader
                        + ", sampleSize="
                        + sampleSize);

        List<String[]> samples = new ArrayList<>();
        List<String> header = null;
        boolean isFirstLine = true;
        int linesRead = 0;

        for (String line : csvLines) {
            if (linesRead >= sampleSize) {
                break;
            }

            // CRITICAL FIX: Only skip completely empty lines when not in multi-line mode
            if ((line == null || line.isEmpty()) && !parser.isInMultiLineState()) {
                System.out.println("DEBUG: Skipping completely empty line");
                continue;
            }

            String[] fields = parser.parseLine(line, false);
            if (fields == null) {
                // Multi-line field, continue reading
                System.out.println("DEBUG: Multi-line field continues to next line");
                continue;
            }

            if (isFirstLine && hasHeader) {
                header = List.of(fields);
                isFirstLine = false;
                System.out.println(
                        "DEBUG: Parsed header with "
                                + fields.length
                                + " fields: "
                                + List.of(fields));
            } else {
                samples.add(fields);
                linesRead++;
                System.out.println(
                        "DEBUG: Parsed " + fields.length + " fields: " + List.of(fields));
            }
        }

        // Handle any pending multi-line record at end - FIXED VERSION
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
                System.out.println(
                        "DEBUG: Parsed final "
                                + finalFields.length
                                + " fields: "
                                + List.of(finalFields));
            }
        }

        CsvSchema schema = analyzeSchema(samples, header);
        printDetailedAnalysis(schema.getColumns().toArray(new CsvColumn[0]), samples);
        return schema;
    }

    /** Infer schema from InputStream with proper multi-line support. */
    public CsvSchema inferSchemaFromStream(InputStream inputStream, boolean hasHeader)
            throws IOException {
        System.out.println(
                "DEBUG: Inferring schema from InputStream, hasHeader="
                        + hasHeader
                        + ", sampleSize="
                        + sampleSize);

        List<String[]> samples = new ArrayList<>();
        List<String> header = null;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int linesRead = 0;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null && linesRead < sampleSize) {
                // CRITICAL FIX: Only skip completely empty lines when not in multi-line mode
                if (line.isEmpty() && !parser.isInMultiLineState()) {
                    System.out.println("DEBUG: Skipping completely empty line");
                    continue;
                }

                String[] fields = parser.parseLine(line, false);

                if (fields == null) {
                    // Multi-line field, continue reading
                    System.out.println("DEBUG: Multi-line field continues to next line");
                    continue;
                }

                if (isFirstLine && hasHeader) {
                    header = List.of(fields);
                    isFirstLine = false;
                    System.out.println(
                            "DEBUG: Parsed header with "
                                    + fields.length
                                    + " fields: "
                                    + List.of(fields));
                } else {
                    samples.add(fields);
                    linesRead++;
                    System.out.println(
                            "DEBUG: Parsed " + fields.length + " fields: " + List.of(fields));
                }
            }

            // Handle any pending multi-line record at end - FIXED VERSION
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
                    System.out.println(
                            "DEBUG: Parsed final "
                                    + finalFields.length
                                    + " fields: "
                                    + List.of(finalFields));
                }
            }
        }

        CsvSchema schema = analyzeSchema(samples, header);
        printDetailedAnalysis(schema.getColumns().toArray(new CsvColumn[0]), samples);
        return schema;
    }

    private CsvSchema analyzeSchema(List<String[]> samples, List<String> header) {
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("No data samples found to infer schema");
        }

        int columnCount = samples.get(0).length;
        CsvColumn[] columns = new CsvColumn[columnCount];

        // Initialize columns
        for (int i = 0; i < columnCount; i++) {
            String columnName =
                    (header != null && i < header.size()) ? header.get(i) : "field_" + i;
            columns[i] = new CsvColumn(columnName);
        }

        // Analyze each sample
        for (String[] sample : samples) {
            if (sample.length != columnCount) {
                System.err.println(
                        "WARNING: Inconsistent column count. Expected "
                                + columnCount
                                + ", got "
                                + sample.length
                                + " in row: "
                                + List.of(sample));
                continue;
            }

            for (int i = 0; i < columnCount; i++) {
                columns[i].analyzeValue(sample[i]);
            }
        }

        // Build schema
        CsvSchema schema = new CsvSchema();
        for (CsvColumn column : columns) {
            schema.addColumn(column);
        }

        return schema;
    }

    /** Method for detailed type analysis. */
    private void printDetailedAnalysis(CsvColumn[] columns, List<String[]> samples) {
        System.out.println("\nDetailed Type Analysis:");
        System.out.println("=" + "=".repeat(100));

        for (int i = 0; i < columns.length; i++) {
            CsvColumn col = columns[i];
            System.out.printf("Column %d: %s%n", i, col.getName());
            System.out.printf("  Final Type: %s%n", col.getInferredType());
            System.out.printf(
                    "  Compatible with: [INT:%-5s DOUBLE:%-5s BOOLEAN:%-5s DATE:%-5s TIME:%-5s TIMESTAMP:%-5s]%n",
                    col.hasSeenInteger(),
                    col.hasSeenDouble(),
                    col.hasSeenBoolean(),
                    col.hasSeenDate(),
                    col.hasSeenTime(),
                    col.hasSeenTimestamp());
            System.out.printf("  Non-null values: %d%n", col.getNonNullCount());

            // Show sample values for this column
            System.out.print("  Sample values: ");
            int samplesShown = 0;
            for (String[] sample : samples) {
                if (i < sample.length && samplesShown < 3) {
                    System.out.print("[" + sample[i] + "] ");
                    samplesShown++;
                }
            }
            System.out.println("\n" + "-".repeat(80));
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        // Test edge case functionality - check if this is the first argument
        if (args[0].equals("--test-edge-cases")) {
            testAllEdgeCases();
            return;
        }

        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }

        String filePath = args[0];
        boolean hasHeader = Boolean.parseBoolean(args[1]);
        int sampleSize = Integer.parseInt(args[2]);
        char delimiter = args.length > 3 ? args[3].charAt(0) : ',';
        boolean allowMultiLine = args.length > 4 ? Boolean.parseBoolean(args[4]) : false;

        try {
            CsvSchemaInferrer inferrer =
                    new CsvSchemaInferrer(hasHeader, sampleSize, delimiter, allowMultiLine);
            CsvSchema schema = inferrer.inferSchema(filePath);
            System.out.println("Inferred Schema:");
            System.out.println(schema);

            System.out.println("\nParser Statistics:");
            System.out.println("  Lines processed: " + inferrer.parser.getLinesProcessed());
            System.out.println("  Fields processed: " + inferrer.parser.getFieldsProcessed());
            System.out.println("  Multi-line state: " + inferrer.parser.isInMultiLineState());

        } catch (Exception e) {
            System.err.println("Error inferring schema: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println(
                "Usage: CsvSchemaInferrer <filePath> <hasHeader> <sampleSize> [delimiter] [allowMultiLine]");
        System.err.println("  filePath: Path to CSV file");
        System.err.println("  hasHeader: true/false whether first line is header");
        System.err.println("  sampleSize: Number of rows to sample");
        System.err.println("  delimiter: (Optional) Field delimiter character, default ','");
        System.err.println(
                "  allowMultiLine: (Optional) true/false whether to handle multi-line fields, default false");
        System.err.println("");
        System.err.println("Alternative usage for testing:");
        System.err.println("  CsvSchemaInferrer --test-edge-cases");
    }

    /** Test all edge cases functionality. */
    private static void testAllEdgeCases() {
        try {
            CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 10, ',', true);

            // Test 1: Trailing empty columns
            System.out.println("=== Test 1: Trailing Empty Columns ===");
            String trailingEmpty = "id,name,age,\n1,John,30,\n2,Jane,25,";
            CsvSchema schema1 = inferrer.inferSchemaFromString(trailingEmpty, true);
            System.out.println(
                    "Columns detected: " + schema1.getColumns().size() + " (should be 4)");

            inferrer.parser.reset();

            // Test 2: Multi-line field preservation
            System.out.println("\n=== Test 2: Multi-line Field Preservation ===");
            String multiLine = "id,description\n1,\"Line 1\nLine 2\"\n2,\"Single line\"";
            CsvSchema schema2 = inferrer.inferSchemaFromString(multiLine, true);

            inferrer.parser.reset();

            // Test 3: Mixed empty columns
            System.out.println("\n=== Test 3: Mixed Empty Columns ===");
            String mixedEmpty = "a,b,c,d\n1,,3,\n,2,,4";
            CsvSchema schema3 = inferrer.inferSchemaFromString(mixedEmpty, true);
            System.out.println(
                    "Columns detected: " + schema3.getColumns().size() + " (should be 4)");

            inferrer.parser.reset();

            // Test 4: Empty lines in multi-line fields
            System.out.println("\n=== Test 4: Empty Lines in Multi-line Fields ===");
            String emptyLineMultiLine = "id,text\n1,\"First line\n\nThird line\"\n2,\"Normal\"";
            CsvSchema schema4 = inferrer.inferSchemaFromString(emptyLineMultiLine, true);

        } catch (Exception e) {
            System.err.println("Edge case tests failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
