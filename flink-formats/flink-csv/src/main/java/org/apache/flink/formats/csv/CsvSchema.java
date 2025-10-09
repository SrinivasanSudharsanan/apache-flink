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

    public String toSqlDdl() {
        StringBuilder ddl = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            CsvColumn column = columns.get(i);
            ddl.append("  ").append(column.getName()).append(" ").append(column.getInferredType());
            if (i < columns.size() - 1) {
                ddl.append(",\n");
            }
        }
        return ddl.toString();
    }

    public boolean hasHeader() {
        // Implement based on your header detection logic
        return !columns.isEmpty() && !columns.get(0).getName().startsWith("field_");
    }
}
