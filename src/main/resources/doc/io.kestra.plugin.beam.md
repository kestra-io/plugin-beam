# How to use the Apache Beam plugin

Run Apache Beam pipelines inside a container from Kestra flows.

## Common properties

`containerImage` defaults to `apache/beam_python3.11_sdk:latest`. `taskRunner` controls where the container runs — defaults to Docker. Add extra Python packages via `requirements`.

## Tasks

`RunPipeline` executes a Beam pipeline — provide the pipeline definition via `file` (a path to a YAML pipeline file) or `definition` (inline YAML). Set `sdk` to `PYTHON` (default) or `JAVA`. Set `beamRunner` to control execution: `DIRECT` (default, local), `DATAFLOW`, `FLINK`, or `SPARK`. Pass generic pipeline options via `options` (a map) and runner-specific configuration via `runnerConfig`. Control pipeline timeout with `pipelineTimeoutSeconds` (default 300).

For `DATAFLOW` runner, set `runnerConfig` with `projectId`, `region`, `tempLocation`, and optionally `serviceAccountKey`. For `FLINK`, set `flinkRestUrl` and optionally `parallelism`. For `SPARK`, set `master`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply runner properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).
