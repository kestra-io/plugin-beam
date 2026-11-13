package io.kestra.plugin.beam;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.beam.config.RunnerConfigHolder;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.validation.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class RunPipelineTest {

    // prints the PYTHONPATH the pipeline command sees, as an output marker
    private static final String PRINT_PYTHONPATH = "sh -c \"printf '::{\\\"outputs\\\":{\\\"pythonpath\\\":\\\"%s\\\"}}::\\n' \\\"\\$PYTHONPATH\\\"\"";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void shouldRenderDefaultsWithRunContext() throws Exception {
        RunPipeline task = RunPipeline.builder()
            .id("beam")
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.DIRECT))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .build();

        var runContext = contextFor(task);

        BeamRunner renderedRunner = runContext.render(task.getBeamRunner()).as(BeamRunner.class).orElseThrow();
        assertEquals(BeamRunner.DIRECT, renderedRunner);
        assertEquals(0, validator.validate(task).size());
    }

    @Test
    void shouldLoadDefinitionFromNamespaceStorage() throws Exception {
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunPipeline configured = RunPipeline.builder()
            .id("beam")
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.DIRECT))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .file(Property.ofValue("placeholder"))
            .build();
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, configured);
        RunContext runContext = runContextFactory.of(flow, configured, execution, taskRun);

        String yaml = "pipeline:\\n  transforms: []";
        URI stored = runContext.storage().namespace().putFile(
            java.nio.file.Path.of("pipeline.yaml"),
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))
        ).getLast().uri();

        RunPipeline withFile = RunPipeline.builder()
            .id(configured.getId())
            .type(configured.getType())
            .beamRunner(configured.getBeamRunner())
            .sdk(configured.getSdk())
            .file(Property.ofValue(stored.toString()))
            .options(configured.getOptions())
            .runnerConfig(configured.getRunnerConfig())
            .requirements(configured.getRequirements())
            .build();

        var loaded = withFile.loadDefinition(runContext);
        assertEquals(yaml, loaded);
    }

    @Test
    void shouldResolveRunnerOptionsForAllRunners() throws Exception {
        Map<String, Object> flinkConfig = Map.of("parallelism", 4);
        Map<String, Object> sparkConfig = Map.of("sparkMaster", "local[2]");
        Map<String, Object> dataflowConfig = Map.of("projectId", "p", "region", "r");

        RunPipeline base = RunPipeline.builder()
            .id("beam")
            .type(RunPipeline.class.getName())
            .runnerConfig(Property.ofValue(flinkConfig))
            .beamRunner(Property.ofValue(BeamRunner.FLINK))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, base);
        RunContext runContext = runContextFactory.of(flow, base, execution, taskRun);

        var flinkHolder = base.resolveRunnerOptions(runContext, BeamRunner.FLINK);
        assertTrue(flinkHolder.config() != null);

        RunPipeline sparkTask = RunPipeline.builder()
            .id(base.getId())
            .type(base.getType())
            .beamRunner(Property.ofValue(BeamRunner.SPARK))
            .sdk(base.getSdk())
            .definition(base.getDefinition())
            .runnerConfig(Property.ofValue(sparkConfig))
            .options(base.getOptions())
            .requirements(base.getRequirements())
            .build();
        var sparkHolder = sparkTask.resolveRunnerOptions(runContextFactory.of(flow, sparkTask, execution, TestsUtils.mockTaskRun(execution, sparkTask)), BeamRunner.SPARK);
        assertTrue(sparkHolder.config() != null);

        RunPipeline dataflowTask = RunPipeline.builder()
            .id(base.getId())
            .type(base.getType())
            .beamRunner(Property.ofValue(BeamRunner.DATAFLOW))
            .sdk(base.getSdk())
            .definition(base.getDefinition())
            .runnerConfig(Property.ofValue(dataflowConfig))
            .options(base.getOptions())
            .requirements(base.getRequirements())
            .build();
        var dataflowHolder = dataflowTask.resolveRunnerOptions(runContextFactory.of(flow, dataflowTask, execution, TestsUtils.mockTaskRun(execution, dataflowTask)), BeamRunner.DATAFLOW);
        assertTrue(dataflowHolder.config() != null);
    }

    @Test
    void shouldApplyDataflowServiceAccountKeyWithJavaSdk() {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.DATAFLOW))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .runnerConfig(Property.ofValue(Map.of("projectId", "p", "serviceAccountKey", "not-a-key")))
            .build();

        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of());
        var runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("serviceAccountKey"));
    }

    @Test
    void javaSdkPassesOptionValuesWithoutShellQuotes() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.DATAFLOW))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .runnerConfig(
                Property.ofValue(
                    Map.of(
                        "projectId", "my-project",
                        "region", "europe-west1",
                        "tempLocation", "gs://my-bucket/tmp",
                        "maxWorkers", 5
                    )
                )
            )
            .build();
        var runContext = contextFor(task);
        var holder = task.resolveRunnerOptions(runContext, BeamRunner.DATAFLOW);

        var options = task.javaPipelineOptions(Map.of("appName", "it's a job, with a=b"), holder).as(DataflowPipelineOptions.class);

        assertEquals("my-project", options.getProject());
        assertEquals("europe-west1", options.getRegion());
        assertEquals("gs://my-bucket/tmp", options.getTempLocation());
        assertEquals(5, options.getMaxNumWorkers());
        assertEquals("it's a job, with a=b", options.getAppName());
    }

    @Test
    void javaSdkUsesJavaFlinkOptionNames() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.FLINK))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .runnerConfig(
                Property.ofValue(
                    Map.of(
                        "executionMode", "[local]",
                        "parallelism", 2,
                        "savepointDir", "/tmp/savepoints",
                        "stateBackend", "rocksdb",
                        "stateBackendStoragePath", "file:///tmp/state",
                        "jarPath", "/tmp/job.jar"
                    )
                )
            )
            .build();
        var runContext = contextFor(task);
        var holder = task.resolveRunnerOptions(runContext, BeamRunner.FLINK);

        var options = task.javaPipelineOptions(Map.of(), holder).as(FlinkPipelineOptions.class);

        assertEquals("[local]", options.getFlinkMaster());
        assertEquals(2, options.getParallelism());
        assertEquals("/tmp/savepoints", options.getSavepointPath());
        assertEquals("rocksdb", options.getStateBackend());
        assertEquals("file:///tmp/state", options.getStateBackendStoragePath());
        assertEquals(List.of("/tmp/job.jar"), options.getFilesToStage());
    }

    @Test
    void javaSdkPassesSparkOptionsWithoutShellQuotes() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.SPARK))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .runnerConfig(Property.ofValue(Map.of("master", "local[2]", "checkpointDir", "/tmp/checkpoints")))
            .build();
        var runContext = contextFor(task);
        var holder = task.resolveRunnerOptions(runContext, BeamRunner.SPARK);

        var options = task.javaPipelineOptions(Map.of(), holder).as(SparkPipelineOptions.class);

        assertEquals("local[2]", options.getSparkMaster());
        assertEquals("/tmp/checkpoints", options.getCheckpointDir());
    }

    @Test
    void pythonSdkStillShellQuotesOptionValues() throws Exception {
        var bin = Files.createTempDirectory("fake-python");
        var python = Files.writeString(bin.resolve("python3"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$(dirname \"$0\")/args.txt\"\n");
        Files.setPosixFilePermissions(python, PosixFilePermissions.fromString("rwxr-xr-x"));
        var value = "it's a job; echo injected, a=b";

        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .taskRunner(Process.instance())
            .sdk(Property.ofValue(BeamSDK.PYTHON))
            .beamRunner(Property.ofValue(BeamRunner.DIRECT))
            .definition(Property.ofValue("pipeline: {}"))
            .env(Property.ofValue(Map.of("PATH", "%s:%s".formatted(bin, System.getenv("PATH")))))
            .options(Property.ofValue(Map.of("job_name", value)))
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var args = Files.readAllLines(bin.resolve("args.txt"));
        assertTrue(args.contains("--job_name=%s".formatted(value)), args.toString());
    }

    private RunContext contextFor(RunPipeline task) throws Exception {
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of());
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        return runContextFactory.of(flow, task, execution, taskRun);
    }

    @Test
    void userEnvReachesTheWrapper() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .definition(Property.ofValue("pipeline: {}"))
            .env(Property.ofValue(Map.of("MIN_AMOUNT", "100")))
            .build();

        var wrapper = task.buildCommands(contextFor(task), "echo hi");

        assertEquals(Map.of("MIN_AMOUNT", "100"), wrapper.getEnv());
    }

    @Test
    void beforeCommandsReachTheWrapper() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .definition(Property.ofValue("pipeline: {}"))
            .beforeCommands(Property.ofValue(List.of("echo beforeCommands ran")))
            .build();

        var runContext = contextFor(task);
        var wrapper = task.buildCommands(runContext, "echo hi");

        assertEquals(List.of("echo beforeCommands ran"), runContext.render(wrapper.getBeforeCommands()).asList(String.class));
        assertTrue(wrapper.isBeforeCommandsWithOptions());
    }

    @Test
    void failFastAndImageComeFromTheBaseClass() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .definition(Property.ofValue("pipeline: {}"))
            .failFast(Property.ofValue(false))
            .containerImage(Property.ofValue("example/beam:1"))
            .build();

        var runContext = contextFor(task);
        var wrapper = task.buildCommands(runContext, "echo hi");

        assertEquals(false, wrapper.isFailFast());
        assertEquals("example/beam:1", wrapper.getContainerImage());
    }

    @Test
    void timeoutStateIsReportedWhenFailFastIsOn() throws Exception {
        var out = runScript(RunPipeline.pipelineScript("", 1, "sleep 5"), Property.ofValue(true));

        assertEquals(124, out.getExitCode());
        assertEquals("TIMEOUT", out.getVars().get("state"));
    }

    @Test
    void finishedStateIsReportedOnSuccess() throws Exception {
        var out = runScript(RunPipeline.pipelineScript("", 5, "true"), Property.ofValue(true));

        assertEquals(0, out.getExitCode());
        assertEquals("FINISHED", out.getVars().get("state"));
    }

    @Test
    void pipelineFailureKeepsItsExitCode() throws Exception {
        var out = runScript(RunPipeline.pipelineScript("", 5, "sh -c 'exit 3'"), Property.ofValue(true));

        assertEquals(3, out.getExitCode());
        assertEquals(null, out.getVars().get("state"));
    }

    @Test
    void failedPreludeStopsBeforeThePipelineRuns() throws Exception {
        var out = runScript(RunPipeline.pipelineScript("false && ", 5, "echo should-not-run"), Property.ofValue(true));

        assertEquals(1, out.getExitCode());
        assertEquals(null, out.getVars().get("state"));
    }

    @Test
    void timeoutStateIsReportedWithoutFailFastToo() throws Exception {
        var out = runScript(RunPipeline.pipelineScript("", 1, "sleep 5"), Property.ofValue(false));

        assertEquals(124, out.getExitCode());
        assertEquals("TIMEOUT", out.getVars().get("state"));
    }

    @Test
    void beforeCommandsRunAndAFailingOneStopsTheRun() throws Exception {
        var ok = runScript(
            RunPipeline.pipelineScript("", 5, "echo \"$FROM_ENV\""),
            Property.ofValue(true),
            List.of("export FROM_ENV=from-before-commands")
        );
        assertEquals(0, ok.getExitCode());
        assertEquals("FINISHED", ok.getVars().get("state"));

        var failing = runScript(
            RunPipeline.pipelineScript("", 5, "echo pipeline-ran"),
            Property.ofValue(true),
            List.of("exit 7")
        );
        assertEquals(7, failing.getExitCode());
        assertEquals(null, failing.getVars().get("state"));
    }

    @Test
    void javaSdkWarnsThatEnvAndBeforeCommandsAreIgnored() throws Exception {
        var logs = new CopyOnWriteArrayList<LogEntry>();
        var receive = TestsUtils.receive(logQueue, either -> logs.add(either.getLeft()));

        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .env(JacksonMapper.ofJson().readValue("{\"A\": \"{{ secret('NOT_DEFINED') }}\"}", new TypeReference<Property<Map<String, String>>>() {
            }))
            .beforeCommands(JacksonMapper.ofJson().readValue("[\"cd {{ workingDir }}\"]", new TypeReference<Property<List<String>>>() {
            }))
            .build();
        // neither value can be rendered here ({{ workingDir }} and the secret are undefined): the warning must not need them
        task.warnIgnoredScriptProperties(contextFor(task));

        TestsUtils.awaitLog(logs, l -> task.getId().equals(l.getTaskId()) && l.getLevel() == Level.WARN && l.getMessage().contains("`env`"));
        TestsUtils.awaitLog(logs, l -> task.getId().equals(l.getTaskId()) && l.getLevel() == Level.WARN && l.getMessage().contains("`beforeCommands`"));
        receive.blockLast(Duration.ofMillis(100));
    }

    @Test
    void javaSdkWithoutEnvAndBeforeCommandsDoesNotWarn() throws Exception {
        var logs = new CopyOnWriteArrayList<LogEntry>();
        var receive = TestsUtils.receive(logQueue, either -> logs.add(either.getLeft()));

        var plain = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .build();
        // a marker from the same task shows that the queue delivered what this task logged before the check
        var runContext = contextFor(plain);
        plain.warnIgnoredScriptProperties(runContext);
        runContext.logger().info("marker");
        TestsUtils.awaitLog(logs, l -> plain.getId().equals(l.getTaskId()) && "marker".equals(l.getMessage()));

        // only this task's warnings count: other tests log on the same queue
        assertTrue(logs.stream().noneMatch(l -> plain.getId().equals(l.getTaskId()) && l.getLevel() == Level.WARN));
        receive.blockLast(Duration.ofMillis(100));
    }

    @Test
    void imageFallsBackToTheDefaultWhenSetToNull() throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .definition(Property.ofValue("pipeline: {}"))
            .containerImage(Property.ofValue(null))
            .build();

        var wrapper = task.buildCommands(contextFor(task), "echo hi");

        assertEquals("apache/beam_python3.11_sdk:latest", wrapper.getContainerImage());
    }

    @Test
    void requirementsPathIsExtendedWhenBeforeCommandsExportAPythonPath() throws Exception {
        var out = runScript(
            RunPipeline.pipelineScript(RunPipeline.pythonPathExport("/tmp/beam-py-test"), 5, PRINT_PYTHONPATH),
            Property.ofValue(true),
            List.of("export PYTHONPATH=/app/transforms")
        );

        assertEquals("/tmp/beam-py-test:/app/transforms", out.getVars().get("pythonpath"));
    }

    @Test
    void requirementsPathIsAddedToAPythonPathSetThroughEnv() throws Exception {
        var out = runScript(
            RunPipeline.pipelineScript(RunPipeline.pythonPathExport("/tmp/beam-py-test"), 5, PRINT_PYTHONPATH),
            Property.ofValue(true),
            null,
            Map.of("PYTHONPATH", "/app/transforms")
        );

        assertEquals("/tmp/beam-py-test:/app/transforms", out.getVars().get("pythonpath"));
    }

    private ScriptOutput runScript(String script, Property<Boolean> failFast) throws Exception {
        return runScript(script, failFast, null);
    }

    /** Runs the script through the same wrapper RunPipeline uses, on the Process runner. A failing script returns its output instead of throwing. */
    private ScriptOutput runScript(String script, Property<Boolean> failFast, List<String> beforeCommands) throws Exception {
        return runScript(script, failFast, beforeCommands, null);
    }

    private ScriptOutput runScript(String script, Property<Boolean> failFast, List<String> beforeCommands, Map<String, String> env) throws Exception {
        var task = RunPipeline.builder()
            .id(IdUtils.create())
            .type(RunPipeline.class.getName())
            .definition(Property.ofValue("pipeline: {}"))
            .taskRunner(Process.instance())
            .failFast(failFast)
            .beforeCommands(beforeCommands == null ? null : Property.ofValue(beforeCommands))
            .env(env == null ? null : Property.ofValue(env))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        try {
            return task.buildCommands(runContext, script)
                .withLogConsumer(new DefaultLogConsumer(runContext))
                .run();
        } catch (RunnableTaskException e) {
            // a non-zero exit code is thrown, with the output (exit code and state) attached
            return (ScriptOutput) e.getOutput();
        }
    }
}
