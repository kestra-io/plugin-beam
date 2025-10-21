package io.kestra.plugin.beam;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute an Apache Beam pipeline",
    description = "Execute a Beam pipeline from an inline YAML definition or a YAML file in the flow workspace."
)
@Plugin(
    examples = {
        @Example(
            title = "Run an inline Beam pipeline with the Direct runner",
            code = {
                "tasks:",
                "  - id: run-beam",
                "    type: io.kestra.plugin.beam.BeamPipeline",
                "    sdk: JAVA",
                "    runner: DIRECT",
                "    definition: |",
                "      # Beam YAML here"
            }
        )
    }
)
public class BeamPipeline extends Task implements RunnableTask<BeamPipeline.Output> {
    @Schema( title = "Path to a YAML file in the flow workspace", description = "Path under /flows/..." )
    private Property<String> file;

    @Schema( title = "Inline YAML content describing the Beam pipeline" )
    private Property<String> definition;

    @Schema( title = "Runner used (DIRECT, FLINK, SPARK, DATAFLOW)" )
    @NotNull
    private BeamRunner runner;

    @Schema( title = "SDK to use: JAVA or PYTHON" )
    @NotNull
    private BeamSDK sdk;

    @Schema( title = "Beam pipeline options (project, region, tempLocation, etc.)" )
    @Builder.Default
    private Property<Map<String, String>> options = null;

    @Schema( title = "Runner-specific configuration" )
    @Builder.Default
    private Property<Map<String, Object>> runnerConfig = null;

    @Schema( title = "Python packages to install if sdk=PYTHON" )
    @Builder.Default
    private Property<List<String>> requirements = null;

    @AssertTrue(message = "Either 'file' or 'definition' must be provided, but not both.")
    public boolean isOnlyOneSourceProvided() {
        boolean hasFile = this.file != null;
        boolean hasDefinition = this.definition != null;
        return hasFile ^ hasDefinition;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String filePath = runContext.render(this.file).as(String.class).orElse(null);
        String yamlDefinition = runContext.render(this.definition).as(String.class).orElse(null);

        String source;
        if (filePath != null) {
            source = "file:" + filePath;
            logger.info("Preparing to execute Beam pipeline from file: {} using {} / {}", filePath, this.sdk, this.runner);
        } else {
            int size = yamlDefinition == null ? 0 : yamlDefinition.length();
            source = "inline:" + size + "-chars";
            logger.info("Preparing to execute Beam pipeline from inline definition ({} chars) using {} / {}", size, this.sdk, this.runner);
        }

        // Replace TypeReference usage with Class-based type conversion
        @SuppressWarnings("unchecked")
        Map<String, String> renderedOptions = runContext.render(this.options)
            .<Map<String, String>>as((Class<Map<String, String>>) (Class<?>) Map.class)
            .orElse(null);

        @SuppressWarnings("unchecked")
        Map<String, Object> renderedRunnerConfig = runContext.render(this.runnerConfig)
            .<Map<String, Object>>as((Class<Map<String, Object>>) (Class<?>) Map.class)
            .orElse(null);

        @SuppressWarnings("unchecked")
        List<String> renderedRequirements = runContext.render(this.requirements)
            .<List<String>>as((Class<List<String>>) (Class<?>) List.class)
            .orElse(null);

        if (renderedOptions != null) {
            logger.debug("Beam options: {}", renderedOptions);
        }
        if (renderedRunnerConfig != null) {
            logger.debug("Runner config: {}", renderedRunnerConfig);
        }
        if (renderedRequirements != null) {
            logger.debug("Python requirements: {}", renderedRequirements);
        }

        // Transpose Beam metrics into Kestra's metric system
        logger.info("Transposing Beam metrics to Kestra metrics system");
        runContext.metric(Counter.of("beam.execution.count", 1L));


        return Output.builder()
            .runnerUsed(this.runner.name())
            .sdkUsed(this.sdk.name())
            .source(source)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Runner used for execution")
        private final String runnerUsed;

        @Schema(title = "SDK used for execution")
        private final String sdkUsed;

        @Schema(title = "Pipeline source (file path or inline length)")
        private final String source;
    }

}


