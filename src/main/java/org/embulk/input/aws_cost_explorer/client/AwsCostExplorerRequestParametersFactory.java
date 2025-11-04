package org.embulk.input.aws_cost_explorer.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.embulk.input.aws_cost_explorer.PluginTask;
import software.amazon.awssdk.services.costexplorer.model.DateInterval;
import software.amazon.awssdk.services.costexplorer.model.GetCostAndUsageRequest;
import software.amazon.awssdk.services.costexplorer.model.Granularity;
import software.amazon.awssdk.services.costexplorer.model.GroupDefinition;

public class AwsCostExplorerRequestParametersFactory
{
    private AwsCostExplorerRequestParametersFactory()
    {
    }

    /**
     * Create a GetCostAndUsageRequest object from the PluginTask object.
     *
     * @param task PluginTask object
     * @return GetCostAndUsageRequest object
     */
    public static GetCostAndUsageRequest create(PluginTask task)
    {
        GetCostAndUsageRequest.Builder requestBuilder = GetCostAndUsageRequest.builder()
                .timePeriod(DateInterval.builder()
                        .start(task.getStartDate())
                        .end(task.getEndDate())
                        .build())
                .granularity(Granularity.DAILY)
                .metrics(task.getMetrics());

        final List<GroupDefinition> groups = createGroupDefinitionList(task.getGroups());
        if (!groups.isEmpty()) {
            requestBuilder.groupBy(groups);
        }

        return requestBuilder.build();
    }

    private static List<GroupDefinition> createGroupDefinitionList(List<Map<String, String>> groups)
    {
        ArrayList<GroupDefinition> groupDefinitionList = new ArrayList<>();

        for (Map<String, String> group : groups) {
            groupDefinitionList.add(createGroupDefinition(group.get("type"), group.get("key")));
        }

        return groupDefinitionList;
    }

    private static GroupDefinition createGroupDefinition(String groupType, String groupKey)
    {
        return GroupDefinition.builder()
                .type(groupType)
                .key(groupKey)
                .build();
    }
}
