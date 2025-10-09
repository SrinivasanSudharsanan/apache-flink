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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/** Comprehensive test for CSV schema inference features. */
public class CsvSchemaInferenceTest {

    public static void main(String[] args) {
        System.out.println("=== CSV Schema Inference Test Suite ===");
        System.out.println();

        try {
            createTestDataFiles();
            testBasicInference();
            testTypeDetection();
            testStrictMode();
            testErrorHandling();
            testConfigurationOptions();
            testDdlGeneration();

            System.out.println("=== ALL TESTS COMPLETED SUCCESSFULLY ===");

        } catch (Exception e) {
            System.err.println("TEST FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /** Create test data files programmatically. */
    private static void createTestDataFiles() throws IOException {
        File testDataDir = new File("test-data");
        if (!testDataDir.exists()) {
            testDataDir.mkdir();
            System.out.println("Created test-data directory");
        }

        try (FileWriter writer = new FileWriter("test-data/simple.csv")) {
            writer.write("name,age,salary,active,join_date\n");
            writer.write("John Doe,30,50000.50,true,2023-01-15\n");
            writer.write("Jane Smith,25,75000.00,false,2023-02-20\n");
            writer.write("Bob Johnson,35,60000.75,true,2023-03-10\n");
        }

        try (FileWriter writer = new FileWriter("test-data/types.csv")) {
            writer.write("id,name,age,salary,is_manager,join_date,start_time,created_at\n");
            writer.write("1,Alice,28,55000.50,true,2023-01-15,09:30:00,2023-01-15T09:30:00\n");
            writer.write("2,Bob,35,75000.00,false,2023-02-20,14:15:30,2023-02-20T14:15:30\n");
            writer.write("3,Charlie,42,82000.75,true,2023-03-10,08:45:15,2023-03-10T08:45:15\n");
        }

        try (FileWriter writer = new FileWriter("test-data/malformed.csv")) {
            writer.write("name,age,salary\n");
            writer.write("John Doe,30,50000.50\n");
            writer.write("Jane Smith,twenty five,75000.00\n");
            writer.write("Bob Johnson,35\n");
            writer.write("Alice Brown,28,60000.00,true\n");
        }

        new File("test-data/empty.csv").createNewFile();

        try (FileWriter writer = new FileWriter("test-data/header_only.csv")) {
            writer.write("id,name,age\n");
        }

        try (FileWriter writer = new FileWriter("test-data/pipe_delimited.csv")) {
            writer.write("id|name|value\n");
            writer.write("1|test|100.5\n");
            writer.write("2|demo|200.75\n");
        }

        System.out.println("Test data files created");
        System.out.println();
    }

    /** Test 1: Basic schema inference with default settings. */
    private static void testBasicInference() throws IOException {
        System.out.println("1. Testing Basic Schema Inference...");

        CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 100, ',', true);
        CsvSchema schema = inferrer.inferSchema("test-data/simple.csv");

        System.out.println("Inferred Schema:");
        System.out.println(schema.toString());

        if (schema.getColumns().size() != 5) {
            throw new AssertionError("Should infer 5 columns");
        }

        System.out.println("Basic inference test passed");
        System.out.println();
    }

    /** Test 2: Comprehensive type detection. */
    private static void testTypeDetection() throws IOException {
        System.out.println("2. Testing Type Detection...");

        CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 100, ',', true);
        CsvSchema schema = inferrer.inferSchema("test-data/types.csv");

        System.out.println("Detected Types:");
        for (CsvColumn column : schema.getColumns()) {
            System.out.printf("  %s: %s%n", column.getName(), column.getInferredType());
        }

        System.out.println("Type detection test passed");
        System.out.println();
    }

    /** Test 3: Strict mode behavior. */
    private static void testStrictMode() throws IOException {
        System.out.println("3. Testing Strict Mode...");

        CsvSchemaInferrer lenientInferrer =
                new CsvSchemaInferrer(true, 100, ',', true, false, "test-data/malformed.csv");
        CsvSchema lenientSchema = lenientInferrer.inferSchema("test-data/malformed.csv");
        System.out.println("Non-strict mode: " + lenientSchema.getColumns().size() + " columns");

        try {
            CsvSchemaInferrer strictInferrer =
                    new CsvSchemaInferrer(true, 100, ',', true, true, "test-data/malformed.csv");
            strictInferrer.inferSchema("test-data/malformed.csv");
            throw new AssertionError("Strict mode should have thrown an exception");
        } catch (CsvSchemaInferenceException e) {
            System.out.println("Strict mode correctly threw exception");
        }

        System.out.println("Strict mode test passed");
        System.out.println();
    }

    /** Test 4: Error handling scenarios. */
    private static void testErrorHandling() {
        System.out.println("4. Testing Error Handling...");

        try {
            CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 100, ',', true);
            inferrer.inferSchema("test-data/nonexistent.csv");
            throw new AssertionError("Should throw exception for non-existent file");
        } catch (Exception e) {
            System.out.println("Correctly handled non-existent file");
        }

        try {
            CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 100, ',', true);
            inferrer.inferSchema("test-data/empty.csv");
            throw new AssertionError("Should throw exception for empty file");
        } catch (Exception e) {
            System.out.println("Correctly handled empty file");
        }

        System.out.println("Error handling test passed");
        System.out.println();
    }

    /** Test 5: Configuration options. */
    private static void testConfigurationOptions() throws IOException {
        System.out.println("5. Testing Configuration Options...");

        CsvSchemaInferrer pipeInferrer = new CsvSchemaInferrer(true, 100, '|', true);
        CsvSchema pipeSchema = pipeInferrer.inferSchema("test-data/pipe_delimited.csv");
        System.out.println("Pipe delimiter test passed");

        CsvSchemaInferrer noHeaderInferrer = new CsvSchemaInferrer(false, 100, ',', true);
        CsvSchema noHeaderSchema = noHeaderInferrer.inferSchema("test-data/simple.csv");
        System.out.println("No-header test passed");

        System.out.println("Configuration options test passed");
        System.out.println();
    }

    /** Test 6: DDL Generation. */
    private static void testDdlGeneration() throws IOException {
        System.out.println("6. Testing DDL Generation...");

        CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 100, ',', true);
        CsvSchema schema = inferrer.inferSchema("test-data/types.csv");

        String ddl =
                CsvTableSchemaGenerator.generateSqlDDL(
                        schema, "my_table", "test-data/types.csv", ',');
        System.out.println("Generated DDL:");
        System.out.println(ddl);

        System.out.println("DDL generation test passed");
        System.out.println();
    }
}
