package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for CSV inference format. */
@PublicEvolving
public class CsvInferenceFormatOptions {

    public static final ConfigOption<Boolean> HEADER =
            ConfigOptions.key("header")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether the first line is a header row");

    public static final ConfigOption<Integer> SAMPLE_SIZE =
            ConfigOptions.key("sample-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of rows to sample for type inference");

    public static final ConfigOption<Boolean> STRICT_MODE =
            ConfigOptions.key("strict-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail on type inference conflicts");

    private CsvInferenceFormatOptions() {}
}
