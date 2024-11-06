package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.model.GetCostAndUsageResult;
import com.amazonaws.services.costexplorer.model.Group;
import com.amazonaws.services.costexplorer.model.MetricValue;
import com.amazonaws.services.costexplorer.model.ResultByTime;
import java.time.Instant;
import org.embulk.input.aws_cost_explorer.PluginTask;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsCostExplorerResponse
{
    private final GetCostAndUsageResult result;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TimestampFormatter timestampFormatter = TimestampFormatter.builderWithRuby("%Y-%m-%d").build();

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
        Instant timePeriodStart = timestampFormatter.parse(resultsByTime.getTimePeriod().getStart());
        Instant timePeriodEnd = timestampFormatter.parse(resultsByTime.getTimePeriod().getEnd());
        MetricValue metricValue = resultsByTime.getTotal().get(task.getMetrics());

        addRecordToPage(pageBuilder, task, resultsByTime, timePeriodStart, timePeriodEnd, metricValue, null);
    }

    private void addRecordsToPageWithGroupBy(PageBuilder pageBuilder, PluginTask task, ResultByTime resultsByTime)
    {
        Instant timePeriodStart = timestampFormatter.parse(resultsByTime.getTimePeriod().getStart());
        Instant timePeriodEnd = timestampFormatter.parse(resultsByTime.getTimePeriod().getEnd());

        resultsByTime.getGroups().forEach(group -> {
            MetricValue metricValue = group.getMetrics().get(task.getMetrics());

            addRecordToPage(pageBuilder, task, resultsByTime, timePeriodStart, timePeriodEnd, metricValue, group);
        });
    }

    private void addRecordToPage(
            PageBuilder pageBuilder,
            PluginTask task,
            ResultByTime resultsByTime,
            Instant timePeriodStart,
            Instant timePeriodEnd,
            MetricValue metricValue,
            Group group)
    {
        final Schema schema = pageBuilder.getSchema();
        int i = 0;

        // To support v0.9.x, we need to use PageBuilder.setTimestamp(Column, Timestamp), not setTimestamp(Column, Instant).
        // see "Handling date/time and time zones" section in https://dev.embulk.org/topics/get-ready-for-v0.11-and-v1.0.html
        pageBuilder.setTimestamp(schema.getColumn(i++), Timestamp.ofInstant(timePeriodStart));
        pageBuilder.setTimestamp(schema.getColumn(i++), Timestamp.ofInstant(timePeriodEnd));
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
