package io.kestra.plugin.beam.config;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkRunnerConfigTest {

    @Test
    void shouldMapExecutionModeToFlinkMaster() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "[local]"));
    }

    @Test
    void shouldMapFlinkRestUrlToFlinkMaster() {
        var config = FlinkRunnerConfig.builder()
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "http://localhost:8082"));
    }

    @Test
    void shouldApplyExecutionModeAsFlinkMasterWithJavaSdk() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .parallelism(2)
            .build();

        assertThat(config.toJavaPipelineOptions(), not(hasKey("flink_master")));
        assertThat(config.toJavaPipelineOptions(), hasEntry("parallelism", 2));

        var options = PipelineOptionsFactory.fromArgs("--parallelism=2").withValidation().create();
        config.applyTo(options);

        assertThat(options.as(FlinkPipelineOptions.class).getFlinkMaster(), is("[local]"));
    }

    @Test
    void shouldRejectBothExecutionModeAndFlinkRestUrl() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThrows(IllegalArgumentException.class, config::toPipelineOptions);
    }
}
