package io.kestra.plugin.beam.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.auth.oauth2.ServiceAccountCredentials;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

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

    @Schema(
        title = "Service account key used to authenticate with Google Cloud",
        description = "The service account key JSON, or a path to a key file readable by the Kestra worker. " +
            "It is used as the Beam `gcpCredential` when `sdk` is `JAVA`."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
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

    @Override
    public void applyTo(PipelineOptions options) {
        if (serviceAccountKey == null || serviceAccountKey.isBlank()) {
            return;
        }

        // Beam has no option for a key file: the credential must be set as an object.
        // Scope it like Beam's default GcpCredentialFactory does.
        GcpOptions gcpOptions = options.as(GcpOptions.class);
        gcpOptions.setGcpCredential(loadServiceAccountKey().createScoped(gcpOptions.getGcpOauthScopes()));
    }

    private ServiceAccountCredentials loadServiceAccountKey() {
        String key = serviceAccountKey.trim();
        try (
            InputStream in = key.startsWith("{")
                ? new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8))
                : Files.newInputStream(Path.of(key))
        ) {
            return ServiceAccountCredentials.fromStream(in);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Unable to load the Dataflow `serviceAccountKey`: set it to the service account key JSON or to the path of a readable key file.", e
            );
        }
    }
}
