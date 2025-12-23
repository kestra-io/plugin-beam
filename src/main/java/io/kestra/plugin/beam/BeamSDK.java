package io.kestra.plugin.beam;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(title = "Beam SDK used to execute the pipeline")
public enum BeamSDK {
    JAVA,
    PYTHON
}
