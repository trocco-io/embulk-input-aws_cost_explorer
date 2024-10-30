package org.embulk.input.aws_cost_explorer;

import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.aws_cost_explorer.client.AwsCostExplorerClient;
import org.embulk.input.aws_cost_explorer.client.AwsCostExplorerClientFactory;
import org.embulk.input.aws_cost_explorer.client.AwsCostExplorerRequestParametersFactory;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;

public class AwsCostExplorerInputPlugin
        implements InputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    @Override
    public ConfigDiff transaction(final ConfigSource config, final InputPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        GroupsConfigValidator.validate(task.getGroups());

        final Schema schema = createSchema(task);
        final int taskCount = 1; // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    protected Schema createSchema(PluginTask task) {
        Schema.Builder builder = Schema.builder()
                .add("time_period_start", Types.TIMESTAMP)
                .add("time_period_end", Types.TIMESTAMP)
                .add("metrics", Types.STRING);

        addGroupsToSchema(builder, task.getGroups());

        builder
                .add("amount", Types.DOUBLE)
                .add("unit", Types.STRING)
                .add("estimated", Types.BOOLEAN);

        return builder.build();
    }

    private void addGroupsToSchema(Schema.Builder builder, List<Map<String, String>> groups)
    {
        for (int i = 1; i <= groups.size(); i++) {
            builder.add("group_key" + i, Types.STRING);
        }
    }

    @Override
    public ConfigDiff resume(final TaskSource taskSource, final Schema schema, final int taskCount,
            final InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(final TaskSource taskSource, final Schema schema, final int taskCount,
            final List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(final TaskSource taskSource, final Schema schema, final int taskIndex,
            final PageOutput output)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        final AwsCostExplorerClient awsCostExplorerClient = createAwsCostExplorerClient(task);
        final GetCostAndUsageRequest requestParameters = AwsCostExplorerRequestParametersFactory.create(task);

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            awsCostExplorerClient.requestAll(requestParameters).forEach(response -> response.addRecordsToPage(pageBuilder, task));
            pageBuilder.finish();
        }

        return null;
    }

    protected AwsCostExplorerClient createAwsCostExplorerClient(PluginTask task) {
        return AwsCostExplorerClientFactory.create(task);
    }

    @Override
    public ConfigDiff guess(final ConfigSource config)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }
}
