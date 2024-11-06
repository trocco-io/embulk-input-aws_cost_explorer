package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.model.DateInterval;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;
import com.amazonaws.services.costexplorer.model.Granularity;
import com.amazonaws.services.costexplorer.model.GroupDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.embulk.input.aws_cost_explorer.PluginTask;

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
        GetCostAndUsageRequest request = new GetCostAndUsageRequest()
                .withTimePeriod(new DateInterval().withStart(task.getStartDate()).withEnd(task.getEndDate()))
                .withGranularity(Granularity.DAILY)
                .withMetrics(task.getMetrics());

        final List<GroupDefinition> groups = createGroupDefinitionList(task.getGroups());
        if (!groups.isEmpty()) {
            request.withGroupBy(groups);
        }

        return request;
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
        final GroupDefinition group = new GroupDefinition();
        group.setType(groupType);
        group.setKey(groupKey);

        return group;
    }
}
