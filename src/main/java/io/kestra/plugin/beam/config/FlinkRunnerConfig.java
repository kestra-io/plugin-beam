package io.kestra.plugin.beam.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Schema(title = "Flink runner configuration")
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class FlinkRunnerConfig implements RunnerConfig {
    @Schema(
        title = "Execution mode or master endpoint (e.g. [local], [auto], host:port)",
        description = """
            Sets the Beam Flink master, like `flinkRestUrl`. Set only one of the two.
            """
    )
    @PluginProperty(group = "advanced")
    private String executionMode;

    @Schema(title = "Flink REST endpoint or master to submit the job")
    @PluginProperty(group = "connection")
    private String flinkRestUrl;

    @Schema(title = "Parallelism for the pipeline")
    @PluginProperty(group = "execution")
    private Integer parallelism;

    @Schema(title = "Savepoint path to restore state from")
    @PluginProperty(group = "advanced")
    private String savepointDir;

    @Schema(title = "State backend implementation (e.g. rocksdb, filesystem)")
    @PluginProperty(group = "advanced")
    private String stateBackend;

    @Schema(title = "State backend storage path")
    @PluginProperty(group = "advanced")
    private String stateBackendStoragePath;

    @Schema(title = "Optional jar to stage with the job")
    @PluginProperty(group = "advanced")
    private String jarPath;

    @Override
    public Map<String, Object> toPipelineOptions() {
        var options = new LinkedHashMap<String, Object>();

        flinkMaster().ifPresent(value -> options.put("flink_master", value));
        options.putAll(toJavaPipelineOptions());

        return options;
    }

    // The Java SDK gets the Flink master in applyTo, as its arg parser only knows `flinkMaster`.
    @Override
    public Map<String, Object> toJavaPipelineOptions() {
        var options = new LinkedHashMap<String, Object>();

        Optional.ofNullable(parallelism).ifPresent(value -> options.put("parallelism", value));
        Optional.ofNullable(savepointDir).ifPresent(value -> options.put("savepoint_path", value));
        Optional.ofNullable(stateBackend).ifPresent(value -> options.put("state_backend", value));
        Optional.ofNullable(stateBackendStoragePath).ifPresent(value -> options.put("state_backend_storage_path", value));
        Optional.ofNullable(jarPath).ifPresent(value -> options.put("files_to_stage", value));

        return options;
    }

    @Override
    public void applyTo(PipelineOptions options) {
        flinkMaster().ifPresent(value -> options.as(FlinkPipelineOptions.class).setFlinkMaster(value));
    }

    private Optional<String> flinkMaster() {
        if (executionMode != null && flinkRestUrl != null) {
            throw new IllegalArgumentException(
                "Set either `executionMode` or `flinkRestUrl` in the Flink `runnerConfig`, not both: they both set the Beam Flink master."
            );
        }

        return Optional.ofNullable(flinkRestUrl).or(() -> Optional.ofNullable(executionMode));
    }
}
