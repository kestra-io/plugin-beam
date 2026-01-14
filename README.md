<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png" alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img height="25" src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img height="25" src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img height="25" src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 3 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 3 minutes.</i></p>

# Kestra Apache Beam Plugin

> A plugin to execute Apache Beam pipelines via Kestra.

This plugin allows you to execute **Apache Beam YAML** pipelines directly from Kestra. It supports both **Java** and **Python** SDKs and can dispatch jobs to various runners including **Direct**, **Flink**, **Spark**, and **Google Cloud Dataflow**.

Key features:
* **Multi-Runner Support**: Switch between local execution (Direct) and distributed processing (Flink, Spark, Dataflow) by changing a simple property.
* **Dual SDK**: Run pipelines using the Java SDK (embedded) or the Python SDK (via Docker/TaskRunner).
* **Metrics Integration**: Automatically captures Beam Counters, Distributions, and Gauges and exposes them as Kestra metrics.
* **Declarative Pipelines**: Define your Beam pipeline using the Beam YAML syntax inline or from a file.

![Kestra orchestrator](https://kestra.io/video.gif)

## Development

### Prerequisites
- Java 21
- Docker
- Gradle

### Local Development Infrastructure

To test this plugin with distributed runners (Flink and Spark) locally, a helper script is provided to spin up the necessary infrastructure.

**Starting the environment:**
Run the following script to start Flink (JobManager/TaskManager) and Spark (Master/Worker) using Docker Compose. The script includes health checks to ensure services are fully ready before returning.

```bash
./local-setup-unit.sh
```

Accessing Services: Once the script completes, you can access the dashboards:
- Flink Dashboard: http://localhost:56478
- Spark Master UI: http://localhost:56479

### Running tests
```
./gradlew check --parallel
```

### Development

`VSCode`:

Follow the README.md within the `.devcontainer` folder for a quick and easy way to get up and running with developing plugins if you are using VSCode.

`Other IDEs`:

```
./gradlew shadowJar && docker build -t kestra-custom . && docker run --rm -p 8080:8080 kestra-custom server local
```
> [!NOTE]
> You need to relaunch this whole command everytime you make a change to your plugin

go to http://localhost:8080, your plugin will be available to use

## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)

## Apache Beam Tasks

`RunPipeline` executes Beam YAML pipelines inline or from a file, forwards Beam counters, distributions, and gauges into Kestra metrics, and can target Direct, Flink, Spark, or Dataflow runners with the Java or Python SDK.

```yaml
id: beam-direct
namespace: dev.beam

tasks:
  - id: run-beam
    type: io.kestra.plugin.beam.RunPipeline
    sdk: JAVA
    beamRunner: DIRECT
    definition: |
      pipeline:
        transforms:
          - type: ReadFromText
            config:
              path: "data.txt"
          - type: Count
```

## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
