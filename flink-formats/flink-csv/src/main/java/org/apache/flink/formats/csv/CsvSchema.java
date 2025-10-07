package org.apache.flink.formats.csv;

import java.util.ArrayList;
import java.util.List;

public class CsvSchema {
    private final List<CsvColumn> columns = new ArrayList<>();

    public void addColumn(CsvColumn column) {
        columns.add(column);
    }

    public List<CsvColumn> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (CsvColumn column : columns) {
            sb.append("  ")
                    .append(column.getName())
                    .append(" : ")
                    .append(column.getInferredType())
                    .append("\n");
        }
        return sb.toString();
    }
}
