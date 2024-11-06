package org.embulk.input.aws_cost_explorer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestAwsCostExplorerInputPluginTransaction {
    @Rule public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AwsCostExplorerInputPlugin plugin;
    private ConfigSource config;

    @Before
    public void setUp() {
        plugin = new AwsCostExplorerInputPlugin();

        Map<String, String> group1 = new HashMap<>();
        group1.put("type", "DIMENSION");
        group1.put("key", "USAGE_TYPE");
        Map<String, String> group2 = new HashMap<>();
        group2.put("type", "TAG");
        group2.put("key", "Environment");
        config =
                runtime.getExec()
                        .newConfigSource()
                        .set("access_key_id", "dummy-access-key-id")
                        .set("secret_access_key", "dummy-secret-access-key")
                        .set("metrics", "UnblendedCost")
                        .set("groups", Arrays.asList(group1, group2))
                        .set("start_date", "2024-01-01")
                        .set("end_date", "2024-01-31");
    }

    @Test
    public void testTransaction() {
        AwsCostExplorerInputPlugin.Control control = mock(AwsCostExplorerInputPlugin.Control.class);

        plugin.transaction(config, control);

        verify(control, times(1)).run(any(TaskSource.class), any(Schema.class), eq(1));
    }
}
