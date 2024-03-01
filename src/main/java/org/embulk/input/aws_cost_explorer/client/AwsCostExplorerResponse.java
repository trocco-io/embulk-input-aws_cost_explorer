package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.model.GetCostAndUsageResult;
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
        final Schema schema = pageBuilder.getSchema();

        Timestamp timePeriodStart = timestampParser.parse(resultsByTime.getTimePeriod().getStart());
        Timestamp timePeriodEnd = timestampParser.parse(resultsByTime.getTimePeriod().getEnd());
        MetricValue metricValue = resultsByTime.getTotal().get(task.getMetrics());

        int i = 0;
        pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodStart);
        pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodEnd);
        pageBuilder.setString(schema.getColumn(i++), task.getMetrics());

        for (int j = 0; j < task.getGroups().size(); j++) {
            // Fill with empty strings for periods with no records, as groups will be empty arrays for these periods.
            pageBuilder.setString(schema.getColumn(i++), "");
        }

        pageBuilder.setDouble(schema.getColumn(i++), Double.parseDouble(metricValue.getAmount()));
        pageBuilder.setString(schema.getColumn(i++), metricValue.getUnit());
        pageBuilder.setBoolean(schema.getColumn(i), resultsByTime.isEstimated());

        pageBuilder.addRecord();
    }

    private void addRecordsToPageWithGroupBy(PageBuilder pageBuilder, PluginTask task, ResultByTime resultsByTime)
    {
        final Schema schema = pageBuilder.getSchema();

        Timestamp timePeriodStart = timestampParser.parse(resultsByTime.getTimePeriod().getStart());
        Timestamp timePeriodEnd = timestampParser.parse(resultsByTime.getTimePeriod().getEnd());

        resultsByTime.getGroups().forEach(group -> {
            MetricValue metricValue = group.getMetrics().get(task.getMetrics());
            int i = 0;

            pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodStart);
            pageBuilder.setTimestamp(schema.getColumn(i++), timePeriodEnd);
            pageBuilder.setString(schema.getColumn(i++), task.getMetrics());

            for (String key : group.getKeys()) {
                pageBuilder.setString(schema.getColumn(i++), key);
            }

            pageBuilder.setDouble(schema.getColumn(i++), Double.parseDouble(metricValue.getAmount()));
            pageBuilder.setString(schema.getColumn(i++), metricValue.getUnit());
            pageBuilder.setBoolean(schema.getColumn(i), resultsByTime.isEstimated());

            pageBuilder.addRecord();
        });
    }
}
