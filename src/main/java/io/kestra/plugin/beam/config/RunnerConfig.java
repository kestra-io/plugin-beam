package io.kestra.plugin.beam.config;

import java.util.Map;

import org.apache.beam.sdk.options.PipelineOptions;

public interface RunnerConfig {
    /**
     * Convert the configuration into Beam pipeline options map.
     */
    Map<String, Object> toPipelineOptions();

    /**
     * Pipeline options passed as arguments to the Java SDK.
     */
    default Map<String, Object> toJavaPipelineOptions() {
        return toPipelineOptions();
    }

    /**
     * Apply settings that can't be passed as string arguments (e.g. credentials) to the Java SDK pipeline options.
     */
    default void applyTo(PipelineOptions options) {
    }
}
