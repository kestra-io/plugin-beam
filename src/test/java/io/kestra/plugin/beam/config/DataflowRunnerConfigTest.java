package io.kestra.plugin.beam.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Map;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.auth.oauth2.ServiceAccountCredentials;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataflowRunnerConfigTest {
    private static final String CLIENT_EMAIL = "kestra@my-project.iam.gserviceaccount.com";

    @Test
    void shouldUseServiceAccountKeyJsonAsGcpCredential() throws Exception {
        DataflowRunnerConfig config = DataflowRunnerConfig.builder()
            .projectId("my-project")
            .serviceAccountKey(serviceAccountKeyJson())
            .build();

        GcpOptions options = applyTo(config);

        assertThat(options.getGcpCredential(), instanceOf(ServiceAccountCredentials.class));
        ServiceAccountCredentials credential = (ServiceAccountCredentials) options.getGcpCredential();
        assertThat(credential.getClientEmail(), is(CLIENT_EMAIL));
        assertThat(credential.getScopes(), containsInAnyOrder(options.getGcpOauthScopes().toArray()));
    }

    @Test
    void shouldUseServiceAccountKeyFileAsGcpCredential(@TempDir Path tempDir) throws Exception {
        Path keyFile = Files.writeString(tempDir.resolve("sa.json"), serviceAccountKeyJson());
        DataflowRunnerConfig config = DataflowRunnerConfig.builder()
            .projectId("my-project")
            .serviceAccountKey(keyFile.toString())
            .build();

        GcpOptions options = applyTo(config);

        assertThat(((ServiceAccountCredentials) options.getGcpCredential()).getClientEmail(), is(CLIENT_EMAIL));
    }

    @Test
    void shouldFailWhenServiceAccountKeyFileIsMissing(@TempDir Path tempDir) {
        DataflowRunnerConfig config = DataflowRunnerConfig.builder()
            .serviceAccountKey(tempDir.resolve("missing.json").toString())
            .build();

        assertThrows(IllegalArgumentException.class, () -> applyTo(config));
    }

    private static GcpOptions applyTo(DataflowRunnerConfig config) {
        PipelineOptions options = PipelineOptionsFactory.create();
        config.applyTo(options);
        return options.as(GcpOptions.class);
    }

    private static String serviceAccountKeyJson() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        String privateKey = "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder().encodeToString(generator.generateKeyPair().getPrivate().getEncoded())
            + "\n-----END PRIVATE KEY-----\n";

        return JacksonMapper.ofJson().writeValueAsString(
            Map.of(
                "type", "service_account",
                "project_id", "my-project",
                "private_key_id", "test-key-id",
                "private_key", privateKey,
                "client_email", CLIENT_EMAIL,
                "client_id", "123456789",
                "token_uri", "https://oauth2.googleapis.com/token"
            )
        );
    }
}
