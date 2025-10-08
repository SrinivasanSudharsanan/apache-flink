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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit tests for CsvSchemaInferrer. */
public class CsvSchemaInferrerTest {

    @Test
    public void testBasicSchemaInference() throws Exception {
        // Create test file
        java.nio.file.Files.write(
                java.nio.file.Paths.get("test_basic.csv"),
                "name,age,salary,active\nJohn,25,50000.50,true".getBytes());

        CsvSchemaInferrer inferrer = new CsvSchemaInferrer(true, 1000, ',', true);
        CsvSchema schema = inferrer.inferSchema("test_basic.csv");

        assertNotNull(schema);
        assertEquals(4, schema.getColumns().size());

        // Cleanup
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get("test_basic.csv"));
    }

    @Test
    public void testNoHeaderSchemaInference() throws Exception {
        // Create test file without header
        java.nio.file.Files.write(
                java.nio.file.Paths.get("test_no_header.csv"),
                "John,25,50000.50,true\nJane,30,75000.00,false".getBytes());

        CsvSchemaInferrer inferrer = new CsvSchemaInferrer(false, 1000, ',', true);
        CsvSchema schema = inferrer.inferSchema("test_no_header.csv");

        assertNotNull(schema);
        assertEquals(4, schema.getColumns().size());

        // Cleanup
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get("test_no_header.csv"));
    }
}
