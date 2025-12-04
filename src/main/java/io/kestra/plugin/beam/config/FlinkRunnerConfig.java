package io.kestra.plugin.beam.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Schema(title = "Flink runner configuration")
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class FlinkRunnerConfig implements RunnerConfig {
    @Schema(title = "Execution mode or master endpoint (e.g. [local], [auto], host:port)")
    private String executionMode;

    @Schema(title = "Flink REST endpoint or master to submit the job")
    private String flinkRestUrl;

    @Schema(title = "Parallelism for the pipeline")
    private Integer parallelism;

    @Schema(title = "Savepoint path to restore state from")
    private String savepointDir;

    @Schema(title = "State backend implementation (e.g. rocksdb, filesystem)")
    private String stateBackend;

    @Schema(title = "State backend storage path")
    private String stateBackendStoragePath;

    @Schema(title = "Optional jar to stage with the job")
    private String jarPath;

    @Override
    public Map<String, Object> toPipelineOptions() {
        Map<String, Object> options = new LinkedHashMap<>();

        Optional.ofNullable(flinkRestUrl).ifPresent(url -> options.put("flinkMaster", url));
        if (!options.containsKey("flinkMaster")) {
            Optional.ofNullable(executionMode).ifPresent(mode -> options.put("flinkMaster", mode));
        }
        Optional.ofNullable(parallelism).ifPresent(value -> options.put("parallelism", value));
        Optional.ofNullable(savepointDir).ifPresent(value -> options.put("savepointPath", value));
        Optional.ofNullable(stateBackend).ifPresent(value -> options.put("stateBackend", value));
        Optional.ofNullable(stateBackendStoragePath).ifPresent(value -> options.put("stateBackendStoragePath", value));
        Optional.ofNullable(jarPath).ifPresent(value -> options.put("filesToStage", value));

        return options;
    }
}
