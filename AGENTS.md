# Kestra Beam Plugin

## What

- Provides plugin components under `io.kestra.plugin.beam`.
- Includes classes such as `RunPipeline`, `BeamSDK`, `BeamRunner`, `DataflowRunnerConfig`.

## Why

- What user problem does this solve? Teams need to run pipelines against SDKs and dedicated runners including Apache Flink from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Beam steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Beam.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `beam`

Infrastructure dependencies (Docker Compose services):

- `beam-flink-jobserver`
- `flink-jobmanager`
- `flink-taskmanager`

### Key Plugin Classes

- `io.kestra.plugin.beam.RunPipeline`

### Project Structure

```
plugin-beam/
├── src/main/java/io/kestra/plugin/beam/config/
├── src/test/java/io/kestra/plugin/beam/config/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
