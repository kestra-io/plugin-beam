package io.kestra.plugin.beam.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import io.kestra.core.models.annotations.PluginProperty;

@Schema(title = "Dataflow runner configuration")
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class DataflowRunnerConfig implements RunnerConfig {
    @Schema(title = "GCP project id used for the pipeline")
    @PluginProperty(group = "connection")
    private String projectId;

    @Schema(title = "Region where the pipeline should run")
    @PluginProperty(group = "connection")
    private String region;

    @Schema(title = "Temporary location for Dataflow artifacts")
    @PluginProperty(group = "advanced")
    private String tempLocation;

    @Schema(title = "Staging location for Dataflow artifacts")
    @PluginProperty(group = "advanced")
    private String stagingLocation;

    @Schema(title = "Path to a service account key file used for authentication")
    @PluginProperty(group = "connection", secret = true)
    private String serviceAccountKey;

    @Schema(title = "Whether the job should update an existing pipeline with the same name")
    @PluginProperty(group = "advanced")
    private Boolean update;

    @Schema(title = "Maximum number of workers")
    @PluginProperty(group = "execution")
    private Integer maxWorkers;

    @Schema(title = "Dataflow worker machine type")
    @PluginProperty(group = "advanced")
    private String workerMachineType;

    @Schema(title = "Network to use for workers")
    @PluginProperty(group = "advanced")
    private String network;

    @Schema(title = "Subnetwork to use for workers")
    @PluginProperty(group = "advanced")
    private String subnetwork;

    @Override
    public Map<String, Object> toPipelineOptions() {
        Map<String, Object> options = new LinkedHashMap<>();

        Optional.ofNullable(projectId).ifPresent(value -> options.put("project", value));
        Optional.ofNullable(region).ifPresent(value -> options.put("region", value));
        Optional.ofNullable(tempLocation).ifPresent(value -> options.put("tempLocation", value));
        Optional.ofNullable(stagingLocation).ifPresent(value -> options.put("stagingLocation", value));
        Optional.ofNullable(update).ifPresent(value -> options.put("update", value));
        Optional.ofNullable(maxWorkers).ifPresent(value -> options.put("maxNumWorkers", value));
        Optional.ofNullable(workerMachineType).ifPresent(value -> options.put("workerMachineType", value));
        Optional.ofNullable(network).ifPresent(value -> options.put("network", value));
        Optional.ofNullable(subnetwork).ifPresent(value -> options.put("subnetwork", value));

        return options;
    }
}
