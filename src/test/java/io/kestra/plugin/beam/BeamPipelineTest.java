package io.kestra.plugin.beam;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.models.property.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class BeamPipelineTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testDirectRunnerWithInlineDefinition() throws Exception {
        // Setup
        BeamPipeline task = BeamPipeline.builder()
            .id("test-beam")
            .type(BeamPipeline.class.getName())
            .definition(Property.of(
                "transforms:\n" +
                "  - name: ReadFromText\n" +
                "    spec:\n" +
                "      path: /data/input.txt\n" +
                "  - name: CountWords\n" +
                "    spec:\n" +
                "      groupByKey: true\n" +
                "  - name: WriteToText\n" +
                "    spec:\n" +
                "      path: /data/output.txt\n"
            ))
            .runner(BeamRunner.DIRECT)
            .sdk(BeamSDK.JAVA)
            .build();

        RunContext runContext = runContextFactory.of(task, Map.of());

        // Execute
        BeamPipeline.Output output = task.run(runContext);

        // Assert
        assertThat(output, notNullValue());
        assertThat(output.getRunnerUsed(), is("DIRECT"));
        assertThat(output.getSdkUsed(), is("JAVA"));
        assertThat(output.getSource(), containsString("inline:"));
    }

    @Test
    void testWithExternalFile() throws Exception {
        // Setup
        BeamPipeline task = BeamPipeline.builder()
            .id("test-beam-file")
            .type(BeamPipeline.class.getName())
            .file(Property.of("flows/wordcount.yaml"))
            .runner(BeamRunner.DIRECT)
            .sdk(BeamSDK.JAVA)
            .build();

        RunContext runContext = runContextFactory.of(task, Map.of());

        // Execute
        BeamPipeline.Output output = task.run(runContext);

        // Assert
        assertThat(output, notNullValue());
        assertThat(output.getRunnerUsed(), is("DIRECT"));
        assertThat(output.getSdkUsed(), is("JAVA"));
        assertThat(output.getSource(), is("file:flows/wordcount.yaml"));
    }

    @Test
    void testWithRunnerConfig() throws Exception {
        // Setup
        Map<String, Object> runnerConfig = Map.of(
            "flinkRestUrl", "http://localhost:8081",
            "parallelism", 4
        );

        Map<String, String> options = Map.of(
            "project", "test-project",
            "region", "us-central1"
        );

        BeamPipeline task = BeamPipeline.builder()
            .id("test-beam-config")
            .type(BeamPipeline.class.getName())
            .definition(Property.of("transforms: []"))
            .runner(BeamRunner.FLINK)
            .sdk(BeamSDK.JAVA)
            .runnerConfig(Property.of(runnerConfig))
            .options(Property.of(options))
            .build();

        RunContext runContext = runContextFactory.of(task, Map.of());

        // Execute
        BeamPipeline.Output output = task.run(runContext);

        // Assert
        assertThat(output, notNullValue());
        assertThat(output.getRunnerUsed(), is("FLINK"));
        assertThat(output.getSdkUsed(), is("JAVA"));
        assertThat(output.getSource(), containsString("inline:"));
    }
}