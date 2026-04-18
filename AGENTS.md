# Kestra Beam Plugin

## What

- Provides plugin components under `io.kestra.plugin.beam`.
- Includes classes such as `RunPipeline`, `BeamSDK`, `BeamRunner`, `DataflowRunnerConfig`.

## Why

- This plugin integrates Kestra with Apache Beam.
- It provides tasks that run pipelines against SDKs and dedicated runners including Apache Flink.

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
