package org.embulk.input.aws_cost_explorer;

import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
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

import java.util.List;
import java.util.Map;

public class AwsCostExplorerInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("access_key_id")
        String getAccessKeyId();

        @Config("secret_access_key")
        String getSecretAccessKey();

        @Config("metrics")
        @ConfigDefault("\"UnblendedCost\"")
        String getMetrics();

        @Config("groups")
        @ConfigDefault("[]")
        List<Map<String, String>> getGroups();

        @Config("start_date")
        String getStartDate();

        @Config("end_date")
        String getEndDate();
    }

    @Override
    public ConfigDiff transaction(final ConfigSource config, final InputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        GroupsConfigValidator.validate(task.getGroups());

        final Schema schema = createSchema(task);
        final int taskCount = 1; // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    private Schema createSchema(PluginTask task)
    {
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
        return Exec.newConfigDiff();
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
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final AwsCostExplorerClient awsCostExplorerClient = AwsCostExplorerClientFactory.create(task);
        final GetCostAndUsageRequest requestParameters = AwsCostExplorerRequestParametersFactory.create(task);

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            awsCostExplorerClient.requestAll(requestParameters).forEach(response -> response.addRecordsToPage(pageBuilder, task));
            pageBuilder.finish();
        }

        return null;
    }

    @Override
    public ConfigDiff guess(final ConfigSource config)
    {
        return Exec.newConfigDiff();
    }
}
