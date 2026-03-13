# Kestra Beam Plugin

## What

description = 'Apache Beam Plugin for Kestra Exposes 1 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Beam, allowing orchestration of Apache Beam-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
