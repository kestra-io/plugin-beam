package io.kestra.plugin.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.beam.config.*;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
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
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute an Apache Beam pipeline.",
    description = "Runs a Beam pipeline on the selected runner and SDK and forwards Beam metrics to Kestra."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a pipeline from a YAML definition with the Direct runner and the Python SDK to log 3 numbers",
            full = true,
            code = """
                id: beam_direct_python
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.beam.RunPipeline
                    sdk: PYTHON
                    runner: DIRECT
                    definition: |
                      pipeline:
                        type: chain
                        transforms:
                          - type: Create
                            config:
                              elements: [1, 2, 3]
                          - type: LogForTesting
                """
        ),
        @Example(
            title = "Run a pipeline from a YAML file with the Flink runner and the Python SDK",
            code = """
                id: beam_flink_python
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.beam.RunPipeline
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      privileged: true
                      volumes:
                        - /var/run/docker.sock:/var/run/docker.sock
                    sdk: PYTHON
                    runner: FLINK
                    file: "pipelines/pipeline.yaml"
                    options:
                      flink_master: "http://localhost:56478"
                      parallelism: "4"
                      temp_location: "s3://my-bucket/tmp/"
                      environment_type: DOCKER # https://beam.apache.org/documentation/runtime/sdk-harness-config/
                    runnerConfig:
                      flinkRestUrl: "http://localhost:56478"
                    requirements:
                      - apache-beam==2.69.0
                """
        )
    },
    metrics = {
        @Metric(name = "beam.counter", type = Counter.TYPE, description = "Beam counters forwarded with the 'beam.counter.*' prefix"),
        @Metric(name = "beam.distribution", type = Timer.TYPE, description = "Beam distributions forwarded as timers with prefix 'beam.distribution.*'"),
        @Metric(name = "beam.gauge", type = Counter.TYPE, description = "Beam gauges forwarded as counters with prefix 'beam.gauge.*'")
    }
)
public class RunPipeline extends Task implements RunnableTask<RunPipeline.Output> {

    @Schema(title = "Path to a YAML pipeline definition stored in Kestra storage or locally.")
    private Property<String> file;

    @Schema(title = "Inline YAML pipeline definition.")
    private Property<String> definition;

    @Schema(title = "Beam runner to use.")
    @NotNull
    @Builder.Default
    private Property<BeamRunner> runner = Property.ofValue(BeamRunner.DIRECT);

    @Schema(title = "Beam SDK to execute the pipeline.")
    @NotNull
    @Builder.Default
    private Property<BeamSDK> sdk = Property.ofValue(BeamSDK.JAVA);

    @Schema(title = "Generic Beam pipeline options (project, region, tempLocation, etc.).")
    @Builder.Default
    private Property<Map<String, String>> options = Property.ofValue(Collections.emptyMap());

    @Schema(title = "Runner specific configuration.")
    @Builder.Default
    private Property<Map<String, Object>> runnerConfig = Property.ofValue(Collections.emptyMap());

    @Schema(title = "Python requirements to install before running the pipeline when using the PYTHON SDK.")
    @Builder.Default
    private Property<List<String>> requirements = Property.ofValue(Collections.emptyList());

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String containerImage = "python:3.13-slim";

    @Override
    public Output run(RunContext runContext) throws Exception {
        BeamRunner rRunner = runContext.render(this.runner).as(BeamRunner.class).orElse(BeamRunner.DIRECT);
        BeamSDK rSdk = runContext.render(this.sdk).as(BeamSDK.class).orElse(BeamSDK.JAVA);
        String rYaml = loadDefinition(runContext);
        Map<String, String> rOptions = runContext.render(this.options).asMap(String.class, String.class);
        RunnerConfigHolder configHolder = resolveRunnerOptions(runContext, rRunner);
        applyRunnerSideEffects(runContext, rRunner, configHolder);
        Map<String, Object> runnerOptions = configHolder.options();
        List<String> rRequirements = runContext.render(this.requirements).asList(String.class);

        runContext.logger().info(
            "Starting Beam pipeline sdk={} runner={} options={} runnerConfigKeys={}",
            rSdk, rRunner, rOptions.keySet(), runnerOptions.keySet()
        );

        PipelineRunResult result = rSdk == BeamSDK.PYTHON
            ? runWithPython(runContext, rYaml, rRunner, rOptions, runnerOptions, rRequirements)
            : runWithJava(runContext, rYaml, rRunner, rOptions, runnerOptions, configHolder.config());

        return Output.builder()
            .state(result.getState())
            .jobId(result.getJobId())
            .runner(rRunner)
            .sdk(rSdk)
            .counters(result.getMetrics().getCounters())
            .distributions(result.getMetrics().getDistributions())
            .gauges(result.getMetrics().getGauges())
            .build();
    }

    private PipelineRunResult runWithJava(
        RunContext runContext,
        String yamlDefinition,
        BeamRunner runner,
        Map<String, String> options,
        Map<String, Object> runnerOptions,
        RunnerConfig runnerConfig
    ) {
        PipelineOptions pipelineOptions = createPipelineOptions(options, runnerOptions, runner);

        if (runner == BeamRunner.FLINK && runnerConfig instanceof FlinkRunnerConfig flinkConfig) {
            logUnused(
                runContext,
                flinkConfig.getJarPath(),
                "jarPath is staged via filesToStage; Flink runner handles jar staging.");
        }

        if (runner == BeamRunner.SPARK && runnerConfig instanceof SparkRunnerConfig sparkConfig) {
            logUnused(runContext, sparkConfig.getDeployMode(), "deployMode is not directly handled by the Beam Spark runner.");
            logUnused(runContext, sparkConfig.getSparkHome(), "sparkHome is not directly handled by the Beam Spark runner.");
            logUnused(runContext, sparkConfig.getAdditionalArgs(), "additionalArgs are not forwarded automatically, consider using 'options'.");
        }

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        pipeline.apply("yaml", YamlTransform.source(yamlDefinition));

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        MetricQueryResults metricQueryResults = pipelineResult.metrics().queryMetrics(MetricsFilter.builder().build());
        BeamMetricSnapshot metrics = publishMetrics(runContext, metricQueryResults);

        return PipelineRunResult.builder()
            .state(pipelineResult.getState() != null ? pipelineResult.getState().toString() : "UNKNOWN")
            .jobId("FIXME") // FIXME
            .metrics(metrics)
            .build();
    }

    private PipelineOptions createPipelineOptions(
        Map<String, String> options,
        Map<String, Object> runnerOptions,
        BeamRunner runner
    ) {
        List<String> args = buildOptionArgs(options, runnerOptions);
        PipelineOptions pipelineOptions = PipelineOptionsFactory
            .fromArgs(args.toArray(new String[0]))
            .withValidation()
            .create();
        pipelineOptions.setRunner(resolveRunnerClass(runner));
        return pipelineOptions;
    }

    private PipelineRunResult runWithPython(
        RunContext runContext,
        String yamlDefinition,
        BeamRunner runner,
        Map<String, String> options,
        Map<String, Object> runnerOptions,
        List<String> requirements
    ) throws Exception {
        Path yamlFile = runContext.workingDir().createFile("pipeline.yaml", yamlDefinition.getBytes(StandardCharsets.UTF_8));

        Path venvDir = runContext.workingDir().path().resolve(".venv-beam");
        Path venvPython = venvDir.resolve("bin").resolve("python");

        Map<String, String> env = new HashMap<>();
        env.put("APACHE_BEAM_HOME", runContext.workingDir().path().resolve(".apache_beam").toString());

        // 1) create venv
        runCommand(runContext, "venv", env, String.join(" ",
            "python3", "-m", "venv", shellQuote(venvDir.toString())
        ));

        // 2) bootstrap pip
        runCommand(runContext, "pip", env, String.join(" ",
            shellQuote(venvPython.toString()), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"
        ));

        // 3) install deps (always include Beam YAML support)
        List<String> toInstall = new ArrayList<>();
        toInstall.add("apache-beam[yaml]==2.69.0"); // FIXME: make the version configurable
        if (requirements != null) {
            toInstall.addAll(requirements);
        }

        String pipInstallCmd =
            shellQuote(venvPython.toString())
                + " -m pip install "
                + toInstall.stream().map(this::shellQuote).collect(Collectors.joining(" "));

        runCommand(runContext, "pip", env, pipInstallCmd);

        // 4) run beam yaml cli
        List<String> cmd = new ArrayList<>();
        cmd.add(shellQuote(venvPython.toString()));
        cmd.add("-m");
        cmd.add("apache_beam.yaml.main");
        cmd.add("--yaml_pipeline_file");
        cmd.add(shellQuote(yamlFile.toAbsolutePath().toString()));
        cmd.add("--runner");
        cmd.add(shellQuote(resolvePythonRunner(runner)));
        cmd.addAll(buildOptionArgs(options, runnerOptions).stream().map(this::shellQuote).toList());

        runCommand(runContext, "beam-python", env, String.join(" ", cmd));

        runContext.logger().info("Beam Python SDK completed; metrics are not available through the YAML CLI runner.");

        return PipelineRunResult.builder()
            .state("FINISHED")
            .jobId("FIXME") // FIXME
            .metrics(BeamMetricSnapshot.empty())
            .build();
    }

    private void runCommand(RunContext runContext, String label, Map<String, String> env, String command) throws Exception {
        ScriptOutput out = new CommandsWrapper(runContext)
            .withEnv(env)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(List.of(command)))
            .run();

        Integer exit = out == null ? null : out.getExitCode();
        if (exit == null || exit != 0) {
            throw new IllegalStateException("Command failed [" + label + "] exitCode=" + exit + " command=" + command);
        }
    }

    private String shellQuote(String s) {
        if (s == null) {
            return "''";
        }
        // minimal safe single-quote escaping for /bin/sh -c
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

    private BeamMetricSnapshot publishMetrics(RunContext runContext, MetricQueryResults metricQueryResults) {
        Map<String, Long> counters = new LinkedHashMap<>();
        Map<String, Double> distributions = new LinkedHashMap<>();
        Map<String, Double> gauges = new LinkedHashMap<>();

        for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
            String key = metricKey(counter.getKey());
            Long value = counter.getAttempted();
            counters.put(key, value);
            runContext.metric(Counter.of("beam.counter." + key, Objects.requireNonNull(counter.getAttempted()), buildTags(counter.getKey())));
        }

        for (MetricResult<DistributionResult> distribution : metricQueryResults.getDistributions()) {
            String key = metricKey(distribution.getKey());
            DistributionResult result = distribution.getAttempted();
            double mean = Objects.requireNonNull(result).getCount() == 0 ? 0 : result.getMean();
            distributions.put(key, mean);
            Duration duration = Duration.ofMillis(Math.round(mean));
            runContext.metric(Timer.of("beam.distribution." + key, duration, buildTags(distribution.getKey())));
            runContext.metric(Counter.of("beam.distribution." + key + ".count", result.getCount(), buildTags(distribution.getKey())));
        }

        for (MetricResult<GaugeResult> gauge : metricQueryResults.getGauges()) {
            String key = metricKey(gauge.getKey());
            double value = Objects.requireNonNull(gauge.getAttempted()).getValue();
            gauges.put(key, value);
            runContext.metric(Counter.of("beam.gauge." + key, value, buildTags(gauge.getKey())));
        }

        return BeamMetricSnapshot.builder()
            .counters(counters)
            .distributions(distributions)
            .gauges(gauges)
            .build();
    }

    private String metricKey(MetricKey metricKey) {
        MetricName metricName = metricKey.metricName();
        String base = (metricName.getNamespace() == null || metricName.getNamespace().isBlank())
            ? metricName.getName()
            : metricName.getNamespace() + "." + metricName.getName();
        if (metricKey.stepName() != null && !metricKey.stepName().isBlank()) {
            base = base + "." + metricKey.stepName();
        }
        return sanitizeMetricName(base);
    }

    private String[] buildTags(MetricKey metricKey) {
        return metricKey.stepName() == null
            ? new String[0]
            : new String[]{"step", metricKey.stepName()};
    }

    private String sanitizeMetricName(String value) {
        return value.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private List<String> buildOptionArgs(Map<String, String> options, Map<String, Object> runnerOptions) {
        List<String> args = new ArrayList<>();
        options.forEach((key, value) -> args.add("--" + key + "=" + value));
        runnerOptions.forEach((key, value) -> args.add("--" + key + "=" + stringify(value)));
        return args;
    }

    private String stringify(Object value) {
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(this::stringify).collect(Collectors.joining(","));
        }
        ObjectMapper mapper = JacksonMapper.ofJson();
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private RunnerConfigHolder resolveRunnerOptions(RunContext runContext, BeamRunner runner) throws Exception {
        Map<String, Object> config = runContext.render(this.runnerConfig).asMap(String.class, Object.class);
        if (config.isEmpty()) {
            return new RunnerConfigHolder(Collections.emptyMap(), null);
        }

        ObjectMapper mapper = JacksonMapper.ofJson();
        RunnerConfig runnerConfig = switch (runner) {
            case FLINK -> mapper.convertValue(config, FlinkRunnerConfig.class);
            case SPARK -> mapper.convertValue(config, SparkRunnerConfig.class);
            case DATAFLOW -> mapper.convertValue(config, DataflowRunnerConfig.class);
            default -> null;
        };

        if (runnerConfig == null) {
            return new RunnerConfigHolder(Collections.emptyMap(), null);
        }

        return new RunnerConfigHolder(runnerConfig.toPipelineOptions(), runnerConfig);
    }

    private void applyRunnerSideEffects(RunContext runContext, BeamRunner runner, RunnerConfigHolder configHolder) throws IOException {
        if (runner == BeamRunner.DATAFLOW && configHolder.config() instanceof DataflowRunnerConfig dataflowConfig) {
            if (dataflowConfig.getServiceAccountKey() != null) {
                Path keyPath = materializeFile(runContext, dataflowConfig.getServiceAccountKey());
                System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", keyPath.toAbsolutePath().toString());
                runContext.logger().info("Configured GOOGLE_APPLICATION_CREDENTIALS to {}", keyPath);
            }
        }

        if (runner == BeamRunner.SPARK && configHolder.config() instanceof SparkRunnerConfig sparkConfig) {
            if (sparkConfig.getDriverMemory() != null) {
                System.setProperty("spark.driver.memory", sparkConfig.getDriverMemory());
                runContext.logger().debug("Applied spark.driver.memory={}", sparkConfig.getDriverMemory());
            }
            if (sparkConfig.getExecutorMemory() != null) {
                System.setProperty("spark.executor.memory", sparkConfig.getExecutorMemory());
                runContext.logger().debug("Applied spark.executor.memory={}", sparkConfig.getExecutorMemory());
            }
            if (sparkConfig.getExecutorCores() != null) {
                System.setProperty("spark.executor.cores", sparkConfig.getExecutorCores().toString());
                runContext.logger().debug("Applied spark.executor.cores={}", sparkConfig.getExecutorCores());
            }
        }
    }

    private Path materializeFile(RunContext runContext, String location) {
        try {
            URI uri = URI.create(location);
            try (InputStream inputStream = runContext.storage().getFile(uri)) {
                Path temp = runContext.workingDir().createTempFile(".json");
                Files.copy(inputStream, temp, StandardCopyOption.REPLACE_EXISTING);
                return temp;
            }
        } catch (IllegalArgumentException | IOException e) {
            Path path = Paths.get(location);
            if (!path.isAbsolute()) {
                path = runContext.workingDir().resolve(path);
            }
            return path;
        }
    }

    private String loadDefinition(RunContext runContext) throws Exception {
        Optional<String> rDefinition = runContext.render(this.definition).as(String.class);
        if (rDefinition.isPresent()) {
            return rDefinition.get();
        }

        Optional<String> rFile = runContext.render(this.file).as(String.class);
        if (rFile.isEmpty()) {
            throw new IllegalArgumentException("No pipeline definition provided");
        }

        return readFile(runContext, rFile.get());
    }

    private String readFile(RunContext runContext, String path) throws Exception {
        try {
            URI uri = URI.create(path);
            try (InputStream inputStream = runContext.storage().getFile(uri)) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IllegalArgumentException | IOException ignored) {
            // fallthrough
        }

        InputStream resource = RunPipeline.class.getClassLoader().getResourceAsStream(path);
        if (resource != null) {
            try (resource) {
                return new String(resource.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        Path localPath = Paths.get(path);
        if (!localPath.isAbsolute()) {
            localPath = runContext.workingDir().resolve(localPath);
        }
        return Files.readString(localPath);
    }

    private Class<? extends PipelineRunner<?>> resolveRunnerClass(BeamRunner runner) {
        return switch (runner) {
            case DIRECT -> DirectRunner.class;
            case FLINK -> FlinkRunner.class;
            case SPARK -> SparkRunner.class;
            case DATAFLOW -> DataflowRunner.class;
        };
    }

    private String resolvePythonRunner(BeamRunner runner) {
        return switch (runner) {
            case DIRECT -> "DirectRunner";
            case FLINK -> "FlinkRunner";
            case SPARK -> "SparkRunner";
            case DATAFLOW -> "DataflowRunner";
        };
    }

    private void logUnused(RunContext runContext, Object value, String message) {
        if (value != null) {
            runContext.logger().debug(message);
        }
    }

    @Builder
    @Getter
    @AllArgsConstructor
    private static class PipelineRunResult {
        private final String state;
        private final String jobId;
        @Builder.Default
        private final BeamMetricSnapshot metrics = BeamMetricSnapshot.empty();
    }

    @Builder
    @Getter
    @AllArgsConstructor
    private static class BeamMetricSnapshot {
        @Builder.Default
        private final Map<String, Long> counters = Collections.emptyMap();
        @Builder.Default
        private final Map<String, Double> distributions = Collections.emptyMap();
        @Builder.Default
        private final Map<String, Double> gauges = Collections.emptyMap();

        public static BeamMetricSnapshot empty() {
            return BeamMetricSnapshot.builder().build();
        }
    }

    @Builder
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Pipeline state as reported by the runner")
        private String state;

        @Schema(title = "Runner used for this execution")
        private BeamRunner runner;

        @Schema(title = "SDK used for this execution")
        private BeamSDK sdk;

        @Schema(title = "Runner job id when available")
        private String jobId;

        @Schema(title = "Beam counter metrics forwarded to Kestra")
        @Builder.Default
        private Map<String, Long> counters = Collections.emptyMap();

        @Schema(title = "Beam distribution metrics forwarded to Kestra")
        @Builder.Default
        private Map<String, Double> distributions = Collections.emptyMap();

        @Schema(title = "Beam gauge metrics forwarded to Kestra")
        @Builder.Default
        private Map<String, Double> gauges = Collections.emptyMap();
    }
}
