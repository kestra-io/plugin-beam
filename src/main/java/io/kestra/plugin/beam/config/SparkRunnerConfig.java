package io.kestra.plugin.beam.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.util.LinkedHashMap;
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

    @Schema(title = "Optional jar to stage with the job")
    private String jarPath;

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
