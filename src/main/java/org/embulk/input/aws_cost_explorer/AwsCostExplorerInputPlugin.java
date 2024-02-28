package org.embulk.input.aws_cost_explorer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.costexplorer.AWSCostExplorer;
import com.amazonaws.services.costexplorer.AWSCostExplorerClientBuilder;
import com.amazonaws.services.costexplorer.model.DateInterval;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageResult;
import com.amazonaws.services.costexplorer.model.Granularity;
import com.amazonaws.services.costexplorer.model.MetricValue;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public class AwsCostExplorerInputPlugin
        implements InputPlugin
{
    protected final Logger logger = Exec.getLogger(getClass());

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

        ImmutableList.Builder<Column> columns = ImmutableList.builder();

        columns.add(new Column(0, "time_period_start", Types.TIMESTAMP));
        columns.add(new Column(1, "time_period_end", Types.TIMESTAMP));
        columns.add(new Column(2, "metrics", Types.STRING));
        columns.add(new Column(3, "amount", Types.DOUBLE));
        columns.add(new Column(4, "unit", Types.STRING));
        columns.add(new Column(5, "estimated", Types.BOOLEAN));

        final Schema schema = new Schema(columns.build());
        final int taskCount = 1; // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
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

        final AWSCredentials cred = new BasicAWSCredentials(task.getAccessKeyId(), task.getSecretAccessKey());

        final AWSCostExplorer awsCostExplorerClient = AWSCostExplorerClientBuilder.standard().withRegion("us-east-1")
                .withCredentials(new AWSStaticCredentialsProvider(cred)).build();
        GetCostAndUsageRequest request = new GetCostAndUsageRequest()
                .withTimePeriod(new DateInterval().withStart(task.getStartDate()).withEnd(task.getEndDate()))
                .withGranularity(Granularity.DAILY).withMetrics(task.getMetrics());

        GetCostAndUsageResult result = awsCostExplorerClient.getCostAndUsage(request);
        final TimestampParser parser = TimestampParser.of("%Y-%m-%d", "UTC");

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            result.getResultsByTime().forEach(resultsByTime -> {
                System.out.println(resultsByTime.toString());
                logger.info("Cost Explorer API results: {}", resultsByTime);
                String start = resultsByTime.getTimePeriod().getStart();
                String end = resultsByTime.getTimePeriod().getEnd();
                pageBuilder.setTimestamp(schema.getColumn(0), parser.parse(start));
                pageBuilder.setTimestamp(schema.getColumn(1), parser.parse(end));
                pageBuilder.setString(schema.getColumn(2), task.getMetrics());
                MetricValue metricValue = resultsByTime.getTotal().get(task.getMetrics());
                pageBuilder.setDouble(schema.getColumn(3), Double.parseDouble(metricValue.getAmount()));
                pageBuilder.setString(schema.getColumn(4), metricValue.getUnit());
                pageBuilder.setBoolean(schema.getColumn(5), resultsByTime.isEstimated());
                pageBuilder.addRecord();
            });
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
