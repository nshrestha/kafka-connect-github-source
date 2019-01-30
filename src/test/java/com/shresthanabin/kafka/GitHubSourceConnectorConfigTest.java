package com.shresthanabin.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class GitHubSourceConnectorConfigTest {

    private ConfigDef configDef = GitHubSourceConnectorConfig.conf();
    private Map<String, String> config;

    @Before
    public void setUpInitialConfig() {
        config = new HashMap<>();
        config.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "foo");
        config.put(GitHubSourceConnectorConfig.REPO_CONFIG, "bar");
        config.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "2017-04-26T01:23:45Z");
        config.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "100");
        config.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "github-issues");
    }

    @Test
    public void doc() {
        System.out.println(GitHubSourceConnectorConfig.conf().toRst());
    }

    @Test
    public void initialConfigIsValid() {
        assertTrue(configDef.validate(config)
                .stream()
                .allMatch(configValue -> configValue.errorMessages().size() == 0));
    }

    @Test
    public void canReadConfigCorrectly() {
        GitHubSourceConnectorConfig config = new GitHubSourceConnectorConfig(this.config);
        config.getAuthPassword();

    }

    @Test
    public void validateSince() {
        config.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "not-a-date");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.SINCE_CONFIG);
        assertTrue(configValue.errorMessages().size() > 0);
    }

    @Test
    public void validateBatchSize() {
        config.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "-1");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG);
        assertTrue(configValue.errorMessages().size() > 0);

        config.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "101");
        configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG);
        assertTrue(configValue.errorMessages().size() > 0);
    }

    @Test
    public void validateUsername() {
        config.put(GitHubSourceConnectorConfig.AUTH_USERNAME_CONFIG, "username");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.AUTH_USERNAME_CONFIG);
        assertEquals(configValue.errorMessages().size(), 0);
    }

    @Test
    public void validatePassword() {
        config.put(GitHubSourceConnectorConfig.AUTH_PASSWORD_CONFIG, "password");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.AUTH_PASSWORD_CONFIG);
        assertEquals(configValue.errorMessages().size(), 0);
    }

}