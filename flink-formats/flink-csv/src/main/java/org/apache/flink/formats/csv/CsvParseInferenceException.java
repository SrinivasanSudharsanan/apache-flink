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

/** Exception for CSV parsing errors during schema inference. */
public class CsvParseInferenceException extends CsvSchemaInferenceException {
    private final String filePath;
    private final int lineNumber;
    private final String lineContent;

    public CsvParseInferenceException(
            String filePath, int lineNumber, String lineContent, String message) {
        super(
                String.format(
                        "CSV parse error in %s at line %d: %s - Line: '%s'",
                        filePath, lineNumber, message, lineContent));
        this.filePath = filePath;
        this.lineNumber = lineNumber;
        this.lineContent = lineContent;
    }

    public CsvParseInferenceException(
            String filePath, int lineNumber, String lineContent, String message, Throwable cause) {
        super(
                String.format(
                        "CSV parse error in %s at line %d: %s - Line: '%s'",
                        filePath, lineNumber, message, lineContent),
                cause);
        this.filePath = filePath;
        this.lineNumber = lineNumber;
        this.lineContent = lineContent;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public String getLineContent() {
        return lineContent;
    }
}
