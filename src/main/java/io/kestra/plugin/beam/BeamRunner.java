package io.kestra.plugin.beam;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(title = "Supported Beam runners")
public enum BeamRunner {
    DIRECT,
    FLINK,
    SPARK,
    DATAFLOW
}
