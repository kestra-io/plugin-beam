package io.kestra.plugin.beam.config;

import java.util.Map;

public record RunnerConfigHolder(Map<String, Object> options, RunnerConfig config) {
}
