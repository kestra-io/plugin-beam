package io.kestra.plugin.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.beam.config.*;
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
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
                    sdk: PYTHON
                    runner: FLINK
                    file: "{{ workingDir }}/pipeline.yaml"
                    options:
                      tempLocation: "s3://my-bucket/tmp/"
                    runnerConfig:
                      flinkRestUrl: "http://flink-jobmanager:8081"
                      parallelism: 4
                    requirements:
                      - apache-beam[flink]==2.65.0
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

    // FIXME: add metadata.yml

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

    // FIXME add namespaceFiles: enabled: true (see Ben message)

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
            ? runWithPython(runContext, rYaml, rRunner, rOptions, runnerOptions, rRequirements) // FIXME: no runnerConfig passed for Python
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

        // FIXME : BeamRunner.DIRECT and BeamRunner.DATAFLOW are missing

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        runContext.logger().info("Beam YAML bytes={} first200={}",
            yamlDefinition.getBytes(StandardCharsets.UTF_8).length,
            yamlDefinition.substring(0, Math.min(200, yamlDefinition.length())).replace("\n", "\\n")
        );

        pipeline.apply("yaml", YamlTransform.source(yamlDefinition));

        runContext.logger().info("HOME={} user.home={} cwd={}",
            System.getenv("HOME"),
            System.getProperty("user.home"),
            runContext.workingDir().path()
        );

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        MetricQueryResults metricQueryResults = pipelineResult.metrics().queryMetrics(MetricsFilter.builder().build());
        BeamMetricSnapshot metrics = publishMetrics(runContext, metricQueryResults);

        return PipelineRunResult.builder()
            .state(pipelineResult.getState() != null ? pipelineResult.getState().toString() : "UNKNOWN")
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

        // 1) create venv
        Path venvDir = runContext.workingDir().path().resolve(".venv-beam");
        Path venvPython = venvDir.resolve("bin").resolve("python");
        Path venvPip = venvDir.resolve("bin").resolve("pip");

        int code;

        code = runProcess(runContext, List.of("python3", "-m", "venv", venvDir.toString()), runContext.workingDir().path(), "venv");
        if (code != 0) throw new IllegalStateException("Failed to create venv, exit code " + code);

        code = runProcess(runContext, List.of(venvPython.toString(), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"),
            runContext.workingDir().path(), "pip");
        if (code != 0) throw new IllegalStateException("Failed to bootstrap pip, exit code " + code);

        // 2) always install Beam YAML support
        List<String> toInstall = new ArrayList<>();
        toInstall.add("apache-beam[yaml]==2.69.0"); // ou rendre la version configurable
        if (requirements != null) toInstall.addAll(requirements);

        List<String> pipCmd = new ArrayList<>();
        pipCmd.add(venvPip.toString());
        pipCmd.add("install");
        pipCmd.addAll(toInstall);

        code = runProcess(runContext, pipCmd, runContext.workingDir().path(), "pip");
        if (code != 0) throw new IllegalStateException("Failed to install Python deps, exit code " + code);

        // 3) run beam yaml cli with venv python
        List<String> command = new ArrayList<>();
        command.add(venvPython.toString());
        command.add("-m");
        command.add("apache_beam.yaml.main");
        command.add("--yaml_pipeline_file");
        command.add(yamlFile.toAbsolutePath().toString());
        command.add("--runner");
        command.add(resolvePythonRunner(runner));
        command.addAll(buildOptionArgs(options, runnerOptions));

        code = runProcess(runContext, command, runContext.workingDir().path(), "beam-python");
        if (code != 0) throw new IllegalStateException("Beam Python process exited with code " + code);

        runContext.logger().info("Beam Python SDK completed; metrics are not available through the YAML CLI runner.");

        return PipelineRunResult.builder()
            .state("FINISHED")
            .metrics(BeamMetricSnapshot.empty())
            .build();
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

    private void installRequirements(RunContext runContext, List<String> requirements) throws IOException, InterruptedException {
        if (requirements == null || requirements.isEmpty()) {
            return;
        }
        List<String> command = new ArrayList<>();
        command.add("python");
        command.add("-m");
        command.add("pip");
        command.add("install");
        command.addAll(requirements);
        int exitCode = runProcess(runContext, command, runContext.workingDir().path(), "pip");
        if (exitCode != 0) {
            throw new IllegalStateException("Failed to install Python requirements, exit code " + exitCode);
        }
    }

    private int runProcess(RunContext runContext, List<String> command, Path workingDir, String label) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        if (workingDir != null) {
            builder.directory(workingDir.toFile());
            builder.environment().put("APACHE_BEAM_HOME", workingDir.resolve(".apache_beam").toString());
        }
        Process process = builder.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                runContext.logger().info("[{}] {}", label, line);
            }
        }
        return process.waitFor();
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
        } catch (IllegalArgumentException | IOException e) {
            Path localPath = Paths.get(path);
            if (!localPath.isAbsolute()) {
                localPath = runContext.workingDir().resolve(localPath);
            }
            return Files.readString(localPath);
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
