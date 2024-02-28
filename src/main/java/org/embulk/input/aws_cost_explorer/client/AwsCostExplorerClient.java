package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.AWSCostExplorer;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;

public class AwsCostExplorerClient
{
    private final AWSCostExplorer client;

    /**
     * Constructor
     *
     * @param client AWS Cost Explorer client
     */
    public AwsCostExplorerClient(AWSCostExplorer client)
    {
        this.client = client;
    }

    /**
     * Request to AWS Cost Explorer
     *
     * @param requestParameters Request parameters
     * @return Response
     */
    public AwsCostExplorerResponse request(GetCostAndUsageRequest requestParameters)
    {
        return new AwsCostExplorerResponse(client.getCostAndUsage(requestParameters));
    }
}
