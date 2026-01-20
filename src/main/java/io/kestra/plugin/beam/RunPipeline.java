package io.kestra.plugin.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.beam.config.*;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.extensions.yaml.YamlTransform;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Execute an Apache Beam pipeline",
    description = """
        Runs an Apache Beam pipeline with strict separation between SDK, runner, and execution mode.

        ### Supported SDKs
        - **JAVA**: Executes directly using the chosen runner (Flink, Spark, etc.).
        - **PYTHON**: Runs via the Beam YAML framework or as a portable pipeline.

        ### Execution Modes & Containers Topology
        The execution involves up to four distinct container layers to orchestrate the pipeline:
        1. **Kestra (Container A)**: The `RunPipeline` task initiates the job. For Python, it prepares the environment and submits the job.
        2. **JobServer (Container B)**: In portable mode (Python/Flink), this container receives the job definition and serves as a bridge to the cluster.
        3. **TaskManager (Container C)**: The runner's execution engine (e.g., Flink TaskManager) that manages the lifecycle of the data processing.
        4. **Worker (Container D)**: For Python pipelines, the TaskManager invokes a worker to execute the Python code. The behavior of this worker is defined by the `environment_type`.

        ### Portable Mode & Runners
        - **DIRECT**: Local execution within the Kestra task container.
        - **FLINK / SPARK**: Portable execution. The pipeline is serialized and sent to a JobServer (Container B) which orchestrates with the cluster.

        ### Environment Types (`environment_type` in options)
        - **DOCKER**: The TaskManager (C) spawns a Sibling Container (D) via the Docker socket to run the Python SDK harness. Requires mounting `/var/run/docker.sock`.
        - **EXTERNAL**: The TaskManager (C) connects to a pre-existing worker pool (Container D) on a specific port (e.g., 50000).
        - **LOOPBACK**: The TaskManager (C) connects back to the original Kestra task process (Container A) to execute code locally, avoiding new container overhead.

        """
)
@Plugin(
    examples = {
        @Example(
            title = "Direct runner with Python SDK",
            full = true,
            code = """
                id: beam_direct_python
                namespace: company.team

                tasks:
                  - id: run
                    type: io.kestra.plugin.beam.RunPipeline
                    sdk: PYTHON
                    beamRunner: DIRECT
                    definition: |
                      pipeline:
                        type: chain
                        transforms:
                          - type: Create
                            config:
                              elements: [1,2,3]
                          - type: LogForTesting
                """
        ),
        @Example(
            title = "Flink runner with Python SDK (portable, DOCKER)",
            code = """
                id: beam_flink_python
                namespace: company.team

                tasks:
                  - id: write
                    type: io.kestra.plugin.core.storage.Write
                    content: |
                      pipeline:
                        type: chain
                        transforms:
                          - type: Create
                            config:
                              elements: [1, 2, 3]
                          - type: LogForTesting
                    extension: .yaml

                  - id: run_pipeline
                    type: io.kestra.plugin.beam.RunPipeline
                    containerImage: apache/beam_python3.11_sdk:latest
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      privileged: true
                      networkMode: host
                      volumes:
                        - /var/run/docker.sock:/var/run/docker.sock
                    sdk: PYTHON
                    beamRunner: FLINK
                    file: "{{ outputs.write.uri }}"
                    options:
                      job_endpoint: "localhost:8099"
                      environment_type: "DOCKER"
                      environment_config: "apache/beam_python3.11_sdk:latest"
                      parallelism: "1"
                    runnerConfig:
                      flinkRestUrl: "http://localhost:8082"
                """
        )
    },
    metrics = {
        @Metric(name = "beam.counter", type = Counter.TYPE),
        @Metric(name = "beam.distribution", type = Timer.TYPE),
        @Metric(name = "beam.gauge", type = Counter.TYPE)
    }
)
public class RunPipeline extends AbstractExecScript implements io.kestra.core.models.tasks.RunnableTask<ScriptOutput> {

    private static final String DEFAULT_IMAGE = "apache/beam_python3.11_sdk:latest";

    private enum ExecutionMode {
        JAVA_CLASSIC,
        PYTHON_DIRECT,
        PYTHON_PORTABLE
    }

    @Schema(title = "Direct reference to a YAML pipeline file.")
    private Property<String> file;

    @Schema(title = "Inline YAML pipeline definition.")
    private Property<String> definition;

    @Schema(
        title = "Beam SDK to use for the pipeline.",
        description = """
            Determines the execution environment.
            - **PYTHON**: Uses the Beam Python SDK. Supports Beam YAML pipelines. Requires a portable runner (like Flink or Spark) for distributed execution.
            - **JAVA**: Uses the Beam Java SDK. Executes JAR-based pipelines.
            """
    )
    @NotNull
    @Builder.Default
    private Property<BeamSDK> sdk = Property.ofValue(BeamSDK.PYTHON);

    @Schema(
        title = "Beam runner to execute the job.",
        description = """
            The execution engine that will run your pipeline.
            - **DIRECT**: Runs the pipeline locally inside the Kestra task container. Best for development or small datasets.
            - **FLINK / SPARK / DATAFLOW**: Distributed runners. For Python, these require a JobServer (Portable mode) to bridge the Python code with the Java-based execution engine
            """
    )
    @NotNull
    @Builder.Default
    private Property<BeamRunner> beamRunner = Property.ofValue(BeamRunner.DIRECT);

    @Schema(
        title = "Generic Beam pipeline options.",
        description = """
            A map of key-value pairs passed directly to the Beam Pipeline Options.
            Common options include:
            - `project` / `region` / `tempLocation`: For Google Cloud Dataflow.
            - `job_endpoint`: The address of the JobServer (e.g., `localhost:8099`) for portable runners.
            - `environment_type`: Defines how workers are launched (`DOCKER`, `EXTERNAL`, or `LOOPBACK`).
            - `environment_config`: Used with `DOCKER` or `EXTERNAL` to specify the worker container image or address.
            """
    )
    @Builder.Default
    private Property<Map<String, String>> options = Property.ofValue(Collections.emptyMap());

    @Schema(title = "Runner specific configuration.")
    @Builder.Default
    private Property<Map<String, Object>> runnerConfig = Property.ofValue(Collections.emptyMap());

    @Schema(title = "Python requirements to install before running the pipeline when using the PYTHON SDK.")
    @Builder.Default
    private Property<List<String>> requirements = Property.ofValue(Collections.emptyList());

    @Builder.Default
    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(title = "The maximum duration to wait for the pipeline to finish in seconds.")
    @Builder.Default
    private Property<Integer> pipelineTimeoutSeconds = Property.ofValue(300);

    @Override
    public Property<String> getContainerImage() {
        return containerImage;
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        BeamSDK sdk = runContext.render(this.sdk).as(BeamSDK.class).orElse(BeamSDK.PYTHON);
        BeamRunner runner = runContext.render(this.beamRunner).as(BeamRunner.class).orElse(BeamRunner.DIRECT);

        ExecutionMode mode = resolveExecutionMode(sdk, runner);
        Map<String, String> opts = runContext.render(this.options).asMap(String.class, String.class);

        validateConfiguration(mode, opts);

        String yaml = loadDefinition(runContext);
        RunnerConfigHolder cfg = resolveRunnerOptions(runContext, runner);

        Integer rPipelineTimeoutSeconds = runContext.render(this.pipelineTimeoutSeconds).as(Integer.class).orElse(60);

        runContext.logger().info(
            "Running Beam pipeline sdk={} runner={} mode={}",
            sdk, runner, mode
        );

        return switch (mode) {
            case JAVA_CLASSIC -> runJava(runContext, yaml, runner, opts, cfg);
            case PYTHON_DIRECT -> runPythonCli(runContext, yaml, opts, false, rPipelineTimeoutSeconds);
            case PYTHON_PORTABLE -> runPythonPortable(runContext, yaml, opts, cfg, rPipelineTimeoutSeconds);
        };
    }

    private ExecutionMode resolveExecutionMode(BeamSDK sdk, BeamRunner runner) {
        if (sdk == BeamSDK.JAVA) return ExecutionMode.JAVA_CLASSIC;
        if (runner == BeamRunner.DIRECT) return ExecutionMode.PYTHON_DIRECT;
        return ExecutionMode.PYTHON_PORTABLE;
    }

    private void validateConfiguration(ExecutionMode mode, Map<String, String> opts) {
        boolean hasEnv = opts.containsKey("environment_type");

        if (mode == ExecutionMode.PYTHON_PORTABLE && !hasEnv) {
            throw new IllegalArgumentException(
                "environment_type is required for Python portable runners"
            );
        }
        if (mode != ExecutionMode.PYTHON_PORTABLE && hasEnv) {
            throw new IllegalArgumentException(
                "environment_type is only valid for Python portable runners"
            );
        }
    }

    private ScriptOutput runJava(
        RunContext runContext,
        String yaml,
        BeamRunner runner,
        Map<String, String> options,
        RunnerConfigHolder runnerConfigHolder
    ) {

        PipelineOptions po = PipelineOptionsFactory
            .fromArgs(buildOptionArgs(options, runnerConfigHolder.options()).toArray(String[]::new))
            .withValidation()
            .create();

        po.setRunner(resolveRunnerClass(runner));

        Pipeline p = Pipeline.create(po);
        p.apply("yaml", YamlTransform.source(yaml));

        PipelineResult result = p.run();
        result.waitUntilFinish();

        publishMetrics(runContext, result.metrics().queryMetrics(MetricsFilter.builder().build()));
        return ScriptOutput.builder().exitCode(0).build();
    }

    private ScriptOutput runPythonPortable(
        RunContext runContext,
        String yaml,
        Map<String, String> options,
        RunnerConfigHolder runnerConfigHolder,
        Integer pipelineTimeoutSeconds
    ) throws Exception {
        Map<String, String> merged = new HashMap<>(options);
        runnerConfigHolder.options().forEach((k, v) -> merged.put(k, stringify(v)));
        // Portable => requires an externally managed JobServer
        // We do NOT start/stop it here, we only use the endpoint passed in options
        return runPythonCli(runContext, yaml, merged, true, pipelineTimeoutSeconds);
    }

    private ScriptOutput runPythonCli(
        RunContext runContext,
        String yaml,
        Map<String, String> options,
        boolean portable,
        Integer pipelineTimeoutSeconds
    ) throws Exception {

        Path yamlFile = runContext.workingDir().createFile(
            "pipeline.yaml",
            yaml.getBytes(StandardCharsets.UTF_8)
        );

        Map<String, String> env = new HashMap<>();
        Path target = Paths.get("/tmp/beam-py-" + UUID.randomUUID());

        runCommand(runContext, "mkdir", env, "mkdir -p " + shellQuote(target.toString()));

        List<String> reqs = runContext.render(this.requirements).asList(String.class);

        if (!reqs.isEmpty()) {
            runCommand(runContext, "pip", env,
                "python3 -m pip install --no-cache-dir --target " +
                    shellQuote(target.toString()) + " " + String.join(" ", reqs)
            );
            env.put("PYTHONPATH", target.toString());
        } else {
            runContext.logger().info("Using Beam SDK pre-installed in the container image.");
        }

        env.put("PYTHONPATH", target.toString());

        List<String> runPipelineOverPythonCmd = new ArrayList<>();
        runPipelineOverPythonCmd.add("python3 -m apache_beam.yaml.main");
        runPipelineOverPythonCmd.add("--yaml_pipeline_file " + shellQuote(yamlFile.toString()));
        runPipelineOverPythonCmd.add("--runner " + (portable ? "PortableRunner" : "DirectRunner"));

        if (portable) {
            String jobEndpoint = options.get("job_endpoint");
            if (jobEndpoint == null || jobEndpoint.isBlank()) {
                throw new IllegalArgumentException(
                    "job_endpoint is required for portable runners (example: localhost:8099)"
                );
            }
            runPipelineOverPythonCmd.add("--job_endpoint " + shellQuote(jobEndpoint));
        }

        runPipelineOverPythonCmd.addAll(buildOptionArgs(options, Map.of()));

        String finalCmd = "timeout --signal=TERM " + pipelineTimeoutSeconds + " " + String.join(" ", runPipelineOverPythonCmd) + "; rc=$?; " +
            "if [ $rc -eq 0 ]; then echo ''; echo '::{\"outputs\":{\"state\":\"FINISHED\"}}::'; exit 0; " +
            "elif [ $rc -eq 124 ]; then echo ''; echo '::{\"outputs\":{\"state\":\"TIMEOUT\"}}::' >&2; exit 124; " +
            "else exit $rc; fi";

        return runCommandReturnOutput(runContext, "beam-python", env, finalCmd);
    }

    private RunnerConfigHolder resolveRunnerOptions(RunContext runContext, BeamRunner runner) throws Exception {

        Map<String, Object> rRunnerConfig = runContext.render(this.runnerConfig).asMap(String.class, Object.class);

        if (rRunnerConfig.isEmpty()) {
            return new RunnerConfigHolder(Collections.emptyMap(), null);
        }

        ObjectMapper mapper = JacksonMapper.ofJson();
        RunnerConfig rc = switch (runner) {
            case FLINK -> mapper.convertValue(rRunnerConfig, FlinkRunnerConfig.class);
            case SPARK -> mapper.convertValue(rRunnerConfig, SparkRunnerConfig.class);
            case DATAFLOW -> mapper.convertValue(rRunnerConfig, DataflowRunnerConfig.class);
            default -> null;
        };

        return rc == null
            ? new RunnerConfigHolder(Collections.emptyMap(), null)
            : new RunnerConfigHolder(rc.toPipelineOptions(), rc);
    }

    private void runCommand(RunContext runContext, String label, Map<String, String> env, String command) throws Exception {
        ScriptOutput out = runCommandReturnOutput(runContext, label, env, command);
        if (out.getExitCode() != 0) {
            throw new IllegalStateException("Command failed: " + label);
        }
    }

    private ScriptOutput runCommandReturnOutput(RunContext runContext, String label, Map<String, String> env, String command) throws Exception {
        ScriptOutput out = this.commands(runContext)
            .withContainerImage(runContext.render(containerImage).as(String.class).orElse(DEFAULT_IMAGE))
            .withEnv(env)
            .withInterpreter(this.interpreter)
            .withFailFast(runContext.render(this.failFast).as(Boolean.class).orElse(true))
            .withCommands(Property.ofValue(List.of(command)))
            .withLogConsumer(new DefaultLogConsumer(runContext) {
                // To avoid having regular logs from the runner containers displayed as ERROR
                @Override
                public void accept(String line, Boolean isStdErr) {
                    if (line.startsWith("::")) {
                        super.accept(line, isStdErr);
                        return;
                    }

                    if (isStdErr && !line.toLowerCase().contains("error")) {
                        runContext.logger().info(line);
                    } else if (isStdErr) {
                        runContext.logger().error(line);
                    } else {
                        runContext.logger().info(line);
                    }
                }
            })
            .run();

        Integer exit = out == null ? null : out.getExitCode();
        if (exit == null || exit != 0) {
            throw new IllegalStateException("Command failed [" + label + "] exitCode=" + exit + " command=" + command);
        }
        return out;
    }

    private List<String> buildOptionArgs(
        Map<String, String> options,
        Map<String, Object> runnerOptions
    ) {
        List<String> args = new ArrayList<>();
        options.forEach((k, v) -> args.add("--" + k + "=" + v));
        runnerOptions.forEach((k, v) -> args.add("--" + k + "=" + stringify(v)));
        return args;
    }

    private String stringify(Object value) {
        if (value instanceof String s) return s;
        try {
            return JacksonMapper.ofJson().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private String loadDefinition(RunContext runContext) throws Exception {
        String inline = runContext.render(definition).as(String.class).orElse(null);
        if (inline != null) {
            return inline;
        }

        String path = runContext.render(file).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("No pipeline definition provided"));

        URI uri = URI.create(path);
        try (InputStream in = runContext.storage().getFile(uri)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private Class<? extends PipelineRunner<?>> resolveRunnerClass(BeamRunner runner) {
        return switch (runner) {
            case DIRECT -> DirectRunner.class;
            case FLINK -> FlinkRunner.class;
            case SPARK -> SparkRunner.class;
            case DATAFLOW -> DataflowRunner.class;
        };
    }

    private void publishMetrics(RunContext runContext, MetricQueryResults results) {
        for (MetricResult<Long> c : results.getCounters()) {
            runContext.metric(
                Counter.of(
                    "beam.counter." + c.getKey().metricName().getName(),
                    c.getAttempted()
                )
            );
        }
    }

    /**
     * POSIX-ish single-quote escape.
     * Example: abc'd -> 'abc'"'"'d'
     */
    private String shellQuote(String stringToQuote) {
        if (stringToQuote == null) {
            return "''";
        }
        return "'" + stringToQuote.replace("'", "'\"'\"'") + "'";
    }
}
