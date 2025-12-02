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
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Schema(title = "Spark runner configuration")
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class SparkRunnerConfig implements RunnerConfig {
    @Schema(title = "Spark master URL (e.g. local[2], spark://spark-master:7077)")
    private String master;

    @Schema(title = "Spark deploy mode")
    private String deployMode;

    @Schema(title = "Spark home directory to use when submitting jobs")
    private String sparkHome;

    @Schema(title = "Optional jar to stage with the job")
    private String jarPath;

    @Schema(title = "Driver memory (e.g. 2g)")
    private String driverMemory;

    @Schema(title = "Executor memory (e.g. 4g)")
    private String executorMemory;

    @Schema(title = "Executor cores")
    private Integer executorCores;

    @Schema(title = "Additional arguments forwarded to the Spark runner")
    private List<String> additionalArgs;

    @Schema(title = "Checkpoint directory for streaming workloads")
    private String checkpointDir;

    @Override
    public Map<String, Object> toPipelineOptions() {
        Map<String, Object> options = new LinkedHashMap<>();

        Optional.ofNullable(master).ifPresent(value -> options.put("sparkMaster", value));
        Optional.ofNullable(checkpointDir).ifPresent(value -> options.put("checkpointDir", value));
        Optional.ofNullable(jarPath).ifPresent(value -> options.put("filesToStage", value));

        return options;
    }
}
