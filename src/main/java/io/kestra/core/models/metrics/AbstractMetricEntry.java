package io.kestra.core.models.metrics;

public abstract class AbstractMetricEntry<T> {
    public abstract String key();
    public abstract T value();
}
