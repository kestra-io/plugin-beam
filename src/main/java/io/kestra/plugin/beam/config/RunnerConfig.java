package io.kestra.plugin.beam.config;

import java.util.Map;

public interface RunnerConfig {
    /**
     * Convert the configuration into Beam pipeline options map.
     */
    Map<String, Object> toPipelineOptions();
}
