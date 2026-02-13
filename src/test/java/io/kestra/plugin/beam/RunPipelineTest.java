package io.kestra.plugin.beam;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.beam.config.RunnerConfigHolder;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class RunPipelineTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    @Test
    void shouldRenderDefaultsWithRunContext() throws Exception {
        RunPipeline task = RunPipeline.builder()
            .id("beam")
            .type(RunPipeline.class.getName())
            .beamRunner(Property.ofValue(BeamRunner.DIRECT))
            .sdk(Property.ofValue(BeamSDK.JAVA))
            .definition(Property.ofValue("pipeline: {}"))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = runContextFactory.of(flow, task, execution, taskRun);

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

        String loaded = (String) invokePrivate(withFile, "loadDefinition", runContext);
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

        RunnerConfigHolder flinkHolder = (RunnerConfigHolder) invokePrivate(base, "resolveRunnerOptions", runContext, BeamRunner.FLINK);
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
        RunnerConfigHolder sparkHolder = (RunnerConfigHolder) invokePrivate(
            sparkTask,
            "resolveRunnerOptions",
            runContextFactory.of(flow, sparkTask, execution, TestsUtils.mockTaskRun(execution, sparkTask)),
            BeamRunner.SPARK
        );
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
        RunnerConfigHolder dataflowHolder = (RunnerConfigHolder) invokePrivate(
            dataflowTask,
            "resolveRunnerOptions",
            runContextFactory.of(flow, dataflowTask, execution, TestsUtils.mockTaskRun(execution, dataflowTask)),
            BeamRunner.DATAFLOW
        );
        assertTrue(dataflowHolder.config() != null);
    }

    private Object invokePrivate(Object target, String method, Object... args) throws Exception {
        Method found = null;
        for (Method m : target.getClass().getDeclaredMethods()) {
            if (!m.getName().equals(method)) {
                continue;
            }
            if (m.getParameterCount() != args.length) {
                continue;
            }
            boolean assignable = true;
            Class<?>[] params = m.getParameterTypes();
            for (int i = 0; i < params.length; i++) {
                if (!params[i].isAssignableFrom(args[i].getClass())) {
                    assignable = false;
                    break;
                }
            }
            if (assignable) {
                found = m;
                break;
            }
        }
        if (found == null) {
            throw new NoSuchMethodException(method);
        }
        found.setAccessible(true);
        return found.invoke(target, args);
    }
}
