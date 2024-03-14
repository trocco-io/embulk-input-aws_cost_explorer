package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.model.GetCostAndUsageResult;
import com.amazonaws.services.costexplorer.model.Group;
import com.amazonaws.services.costexplorer.model.MetricValue;
import com.amazonaws.services.costexplorer.model.ResultByTime;
import org.embulk.input.aws_cost_explorer.AwsCostExplorerInputPlugin.PluginTask;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsCostExplorerResponse
{
    private final GetCostAndUsageResult result;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TimestampParser timestampParser = TimestampParser.of("%Y-%m-%d", "UTC");

    /**
     * Constructor
     *
     * @param result GetCostAndUsageResult
     */
    public AwsCostExplorerResponse(GetCostAndUsageResult result)
    {
        this.result = result;
    }

    /**
     * Get next page token
     */
    public String getNextPageToken()
    {
        return result.getNextPageToken();
    }

    /**
     * Add records to page
     *
     * @param pageBuilder PageBuilder
     * @param task PluginTask
     */
    public void addRecordsToPage(PageBuilder pageBuilder, PluginTask task)
    {
        result.getResultsByTime().forEach(resultsByTime -> {
            System.out.println(resultsByTime.toString());
            logger.info("Cost Explorer API results: {}", resultsByTime);

            if (resultsByTime.getGroups().isEmpty()) {
                addRecordToPageWithoutGroupBy(pageBuilder, task, resultsByTime);
            }
            else {
                addRecordsToPageWithGroupBy(pageBuilder, task, resultsByTime);
            }
        });
    }

    private void addRecordToPageWithoutGroupBy(PageBuilder pageBuilder, PluginTask task, ResultByTime resultsByTime)
    {
        Timestamp timePeriodStart = timestampParser.parse(resultsByTime.getTimePeriod().getStart());
        Timestamp timePeriodEnd = timestampParser.parse(resultsByTime.getTimePeriod().getEnd());
        MetricValue metricValue = resultsByTime.getTotal().get(task.getMetrics());

        addRecordToPage(pageBuilder, task, resultsByTime, timePeriodStart, timePeriodEnd, metricValue, null);
    }

    private void addRecordsToPageWithGroupBy(PageBuilder pageBuilder, PluginTask task, ResultByTime resultsByTime)
    {
        Timestamp timePeriodStart = timestampParser.parse(resultsByTime.getTimePeriod().getStart());
        Timestamp timePeriodEnd = timestampParser.parse(resultsByTime.getTimePeriod().getEnd());

        resultsByTime.getGroups().forEach(group -> {
            MetricValue metricValue = group.getMetrics().get(task.getMetrics());

            addRecordToPage(pageBuilder, task, resultsByTime, timePeriodStart, timePeriodEnd, metricValue, group);
        });
    }

    private void addRecordToPage(
            PageBuilder pageBuilder,
            PluginTask task,
            ResultByTime resultsByTime,
            Timestamp timePeriodStart,
            Timestamp timePeriodEnd,
            MetricValue metricValue,
            Group group)
    {
        final Schema schema = pageBuilder.getSchema();
        int i = 0;

        pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodStart);
        pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodEnd);
        pageBuilder.setString(schema.getColumn(i++), task.getMetrics());

        for (int j = 0; j < task.getGroups().size(); j++) {
            pageBuilder.setString(schema.getColumn(i++), group != null ? group.getKeys().get(j) : "");
        }

        pageBuilder.setDouble(schema.getColumn(i++), Double.parseDouble(metricValue.getAmount()));
        pageBuilder.setString(schema.getColumn(i++), metricValue.getUnit());
        pageBuilder.setBoolean(schema.getColumn(i), resultsByTime.isEstimated());

        pageBuilder.addRecord();
    }
}
