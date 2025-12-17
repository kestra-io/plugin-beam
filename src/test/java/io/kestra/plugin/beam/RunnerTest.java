package io.kestra.plugin.beam;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest {

    @Test
    @ExecuteFlow("sanity-checks/beam_direct_python.yaml")
    void beam_direct_python(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(2));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow(value = "sanity-checks/beam_flink_python.yaml", timeout = "PT5M")
    void beam_flink_python(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(10));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow(value = "sanity-checks/beam_spark_python.yaml", timeout = "PT5M")
    void beam_spark_python(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(10));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
