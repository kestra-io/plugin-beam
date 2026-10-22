package io.kestra.plugin.beam.config;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkRunnerConfigTest {

    @Test
    void shouldMapExecutionModeToFlinkMaster() {
        FlinkRunnerConfig config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "[local]"));
    }

    @Test
    void shouldMapFlinkRestUrlToFlinkMaster() {
        FlinkRunnerConfig config = FlinkRunnerConfig.builder()
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "http://localhost:8082"));
    }

    @Test
    void shouldRejectBothExecutionModeAndFlinkRestUrl() {
        FlinkRunnerConfig config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThrows(IllegalArgumentException.class, config::toPipelineOptions);
    }
}
