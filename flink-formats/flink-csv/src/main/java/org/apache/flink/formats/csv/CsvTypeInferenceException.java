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

/** Exception for type inference conflicts. */
public class CsvTypeInferenceException extends CsvSchemaInferenceException {
    private final String columnName;
    private final String expectedType;
    private final String actualValue;

    public CsvTypeInferenceException(String columnName, String expectedType, String actualValue) {
        super(
                String.format(
                        "Type inference conflict in column '%s': Expected %s, but found value: '%s'",
                        columnName, expectedType, actualValue));
        this.columnName = columnName;
        this.expectedType = expectedType;
        this.actualValue = actualValue;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getExpectedType() {
        return expectedType;
    }

    public String getActualValue() {
        return actualValue;
    }
}
