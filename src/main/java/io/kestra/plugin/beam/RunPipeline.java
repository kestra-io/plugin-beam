package io.kestra.plugin.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
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
                    beamRunner: DIRECT
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

                  - id: upload
                    type: io.kestra.plugin.core.namespace.UploadFiles
                    filesMap:
                      pipeline.yaml: "{{ outputs.write.uri }}"
                    namespace: "{{ flow.namespace }}"

                  - id: run_pipeline
                    type: io.kestra.plugin.beam.RunPipeline
                    namespaceFiles:
                      enabled: true
                      include:
                        - pipeline.yaml
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      privileged: true
                      networkMode: host
                      volumes:
                        - /var/run/docker.sock:/var/run/docker.sock
                    containerImage: python:3.13-slim
                    sdk: PYTHON
                    beamRunner: FLINK
                    file: "{{ outputs.write.uri }}"
                    options:
                      flink_master: "http://localhost:56478"
                      parallelism: "4"
                      temp_location: "s3://my-bucket/tmp/"
                      environment_type: DOCKER
                    runnerConfig:
                      flinkRestUrl: "http://localhost:56478"
                    requirements:
                      - apache-beam==2.69.0
                """
        ),
        @Example(
            title = "Run a pipeline from a YAML file with the Spark runner and the Python SDK",
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

                  - id: upload
                    type: io.kestra.plugin.core.namespace.UploadFiles
                    filesMap:
                      pipeline.yaml: "{{ outputs.write.uri }}"
                    namespace: "{{ flow.namespace }}"

                  - id: run_pipeline
                    type: io.kestra.plugin.beam.RunPipeline
                    namespaceFiles:
                      enabled: true
                      include:
                        - pipeline.yaml
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      privileged: true
                      networkMode: host
                      volumes:
                        - /var/run/docker.sock:/var/run/docker.sock
                    containerImage: python:3.13-slim
                    sdk: PYTHON
                    beamRunner: SPARK
                    file: "{{ outputs.write.uri }}"
                    options:
                      spark_master: "spark://localhost:7077"
                      environment_type: DOCKER
                      temp_location: "s3://my-bucket/tmp/"
                    runnerConfig:
                      master: "spark://localhost:7077"
                    requirements:
                      - apache-beam[spark]==2.69.0
                """
        )
    },
    metrics = {
        @Metric(name = "beam.counter", type = Counter.TYPE, description = "Beam counters forwarded with the 'beam.counter.*' prefix"),
        @Metric(name = "beam.distribution", type = Timer.TYPE, description = "Beam distributions forwarded as timers with prefix 'beam.distribution.*'"),
        @Metric(name = "beam.gauge", type = Counter.TYPE, description = "Beam gauges forwarded as counters with prefix 'beam.gauge.*'")
    }
)
public class RunPipeline extends AbstractExecScript implements io.kestra.core.models.tasks.RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "python:3.13-slim";

    @Schema(title = "Path to a YAML pipeline definition stored in Kestra storage or locally.")
    private Property<String> file;

    @Schema(title = "Inline YAML pipeline definition.")
    private Property<String> definition;

    @Schema(title = "Beam runner to use.")
    @NotNull
    @Builder.Default
    private Property<BeamRunner> beamRunner = Property.ofValue(BeamRunner.DIRECT);

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

    @Builder.Default
    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Override
    public Property<String> getContainerImage() {
        return this.containerImage;
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        BeamRunner rBeamRunner = runContext.render(this.beamRunner).as(BeamRunner.class).orElse(BeamRunner.DIRECT);
        BeamSDK rSdk = runContext.render(this.sdk).as(BeamSDK.class).orElse(BeamSDK.JAVA);
        String rYaml = loadDefinition(runContext);

        Map<String, String> rOptions = runContext.render(this.options).asMap(String.class, String.class);
        RunnerConfigHolder configHolder = resolveRunnerOptions(runContext, rBeamRunner);
        applyRunnerSideEffects(runContext, rBeamRunner, configHolder);

        Map<String, Object> runnerOptions = configHolder.options();
        List<String> rRequirements = runContext.render(this.requirements).asList(String.class);

        runContext.logger().info(
            "Starting Beam pipeline sdk={} runner={} options={} runnerConfigKeys={}",
            rSdk, rBeamRunner, rOptions.keySet(), runnerOptions.keySet()
        );

        if (rSdk == BeamSDK.PYTHON) {
            return runWithPython(runContext, rYaml, rBeamRunner, rOptions, runnerOptions, rRequirements);
        }

        runWithJava(runContext, rYaml, rBeamRunner, rOptions, runnerOptions, configHolder.config());

        // minimal ScriptOutput for JAVA SDK path
        return ScriptOutput.builder().exitCode(0).build();
    }

    private void runWithJava(
        RunContext runContext,
        String yamlDefinition,
        BeamRunner runner,
        Map<String, String> options,
        Map<String, Object> runnerOptions,
        RunnerConfig runnerConfig
    ) {
        PipelineOptions pipelineOptions = createPipelineOptions(options, runnerOptions, runner);

        if (runner == BeamRunner.FLINK && runnerConfig instanceof FlinkRunnerConfig flinkConfig) {
            logUnused(runContext, flinkConfig.getJarPath(), "jarPath is staged via filesToStage; Flink runner handles jar staging.");
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
        publishMetrics(runContext, metricQueryResults);
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

    private ScriptOutput runWithPython(
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
        runCommand(runContext, "pip-bootstrap", env, String.join(" ",
            shellQuote(venvPython.toString()), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"
        ));

        // 3) install deps (always include Beam YAML support)
        List<String> toInstall = new ArrayList<>();
        toInstall.add("apache-beam[yaml]==2.69.0");
        if (requirements != null) {
            toInstall.addAll(requirements);
        }

        String pipInstallCmd =
            shellQuote(venvPython.toString())
                + " -m pip install "
                + toInstall.stream().map(this::shellQuote).collect(Collectors.joining(" "));

        runCommand(runContext, "pip-install", env, pipInstallCmd);

        // 4) run beam yaml cli + publish vars via stdout marker
        List<String> cmd = new ArrayList<>();
        cmd.add(shellQuote(venvPython.toString()));
        cmd.add("-m");
        cmd.add("apache_beam.yaml.main");
        cmd.add("--yaml_pipeline_file");
        cmd.add(shellQuote(yamlFile.toAbsolutePath().toString()));
        cmd.add("--runner");
        cmd.add(shellQuote(resolvePythonRunner(runner)));
        cmd.addAll(buildOptionArgs(options, runnerOptions).stream().map(this::shellQuote).toList());

        String finalCmd = String.join(" ", cmd)
            + " && echo '::{\"outputs\":{\"state\":\"FINISHED\"}}::'";

        return runCommandReturnOutput(runContext, "beam-python", env, finalCmd);
    }

    private void runCommand(RunContext runContext, String label, Map<String, String> env, String command) throws Exception {
        ScriptOutput out = runCommandReturnOutput(runContext, label, env, command);
        Integer exit = out == null ? null : out.getExitCode();
        if (exit == null || exit != 0) {
            throw new IllegalStateException("Command failed [" + label + "] exitCode=" + exit + " command=" + command);
        }
    }

    private ScriptOutput runCommandReturnOutput(RunContext runContext, String label, Map<String, String> env, String command) throws Exception {
        ScriptOutput out = this.commands(runContext)
            .withEnv(env)
            .withInterpreter(this.interpreter)
            .withFailFast(runContext.render(this.failFast).as(Boolean.class).orElse(true))
            .withCommands(Property.ofValue(List.of(command)))
            .run();

        Integer exit = out == null ? null : out.getExitCode();
        if (exit == null || exit != 0) {
            throw new IllegalStateException("Command failed [" + label + "] exitCode=" + exit + " command=" + command);
        }
        return out;
    }

    private String shellQuote(String s) {
        if (s == null) {
            return "''";
        }
        // minimal safe single-quote escaping for /bin/sh -c
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

    private void publishMetrics(RunContext runContext, MetricQueryResults metricQueryResults) {
        for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
            String key = metricKey(counter.getKey());
            runContext.metric(io.kestra.core.models.executions.metrics.Counter.of(
                "beam.counter." + key,
                Objects.requireNonNull(counter.getAttempted()),
                buildTags(counter.getKey())
            ));
        }

        for (MetricResult<DistributionResult> distribution : metricQueryResults.getDistributions()) {
            String key = metricKey(distribution.getKey());
            DistributionResult result = distribution.getAttempted();
            if (result == null) {
                continue;
            }

            double mean = result.getCount() == 0 ? 0 : result.getMean();
            Duration duration = Duration.ofMillis(Math.round(mean));
            runContext.metric(io.kestra.core.models.executions.metrics.Timer.of(
                "beam.distribution." + key,
                duration,
                buildTags(distribution.getKey())
            ));
            runContext.metric(io.kestra.core.models.executions.metrics.Counter.of(
                "beam.distribution." + key + ".count",
                result.getCount(),
                buildTags(distribution.getKey())
            ));
        }

        for (MetricResult<GaugeResult> gauge : metricQueryResults.getGauges()) {
            String key = metricKey(gauge.getKey());
            GaugeResult attempted = gauge.getAttempted();
            if (attempted == null) {
                continue;
            }
            runContext.metric(io.kestra.core.models.executions.metrics.Counter.of(
                "beam.gauge." + key,
                attempted.getValue(),
                buildTags(gauge.getKey())
            ));
        }
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
        // 1) storage URI
        try {
            URI uri = URI.create(path);
            try (InputStream inputStream = runContext.storage().getFile(uri)) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IllegalArgumentException | IOException ignored) {
            // fallthrough
        }

        // 2) classpath (tests / resources)
        InputStream resource = RunPipeline.class.getClassLoader().getResourceAsStream(path);
        if (resource != null) {
            try (resource) {
                return new String(resource.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        // 3) local filesystem (including namespaceFiles injected into workingDir)
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
}
