package org.embulk.input.aws_cost_explorer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.stream.Stream;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.aws_cost_explorer.client.AwsCostExplorerClient;
import org.embulk.input.aws_cost_explorer.client.AwsCostExplorerResponse;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestAwsCostExplorerInputPluginRun {
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    @Rule public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AwsCostExplorerInputPlugin plugin;
    private AwsCostExplorerClient client;
    private ConfigSource config;
    private PageOutput output;

    @Before
    public void setUp() {
        client = mock(AwsCostExplorerClient.class);
        plugin =
                new AwsCostExplorerInputPlugin() {
                    @Override
                    protected AwsCostExplorerClient createAwsCostExplorerClient(PluginTask task) {
                        return client;
                    }
                };

        config =
                runtime.getExec()
                        .newConfigSource()
                        .set("access_key_id", "dummy-access-key-id")
                        .set("secret_access_key", "dummy-secret-access-key")
                        .set("start_date", "2024-01-01")
                        .set("end_date", "2024-01-31");

        output = mock(PageOutput.class);
    }

    @Test
    public void testRun() throws Exception {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        Schema schema = plugin.createSchema(task);
        AwsCostExplorerResponse response = mock(AwsCostExplorerResponse.class);
        when(client.requestAll(any())).thenReturn(Stream.of(response));

        plugin.run(task.toTaskSource(), schema, 0, output);

        verify(response, times(1)).addRecordsToPage(any(), eq(task));
    }
}
