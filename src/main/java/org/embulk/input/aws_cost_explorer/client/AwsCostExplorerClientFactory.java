package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.costexplorer.AWSCostExplorer;
import com.amazonaws.services.costexplorer.AWSCostExplorerClientBuilder;
import org.embulk.input.aws_cost_explorer.PluginTask;

public class AwsCostExplorerClientFactory
{
    private static final String REGION = "us-east-1";

    private AwsCostExplorerClientFactory()
    {
    }

    /**
     * Create a new AwsCostExplorerClient instance.
     *
     * @param task PluginTask
     * @return AwsCostExplorerClient
     */
    public static AwsCostExplorerClient create(PluginTask task)
    {
        final AWSCostExplorer client = AWSCostExplorerClientBuilder
                .standard()
                .withRegion(REGION)
                .withCredentials(createAWSStaticCredentialsProvider(task))
                .build();

        return new AwsCostExplorerClient(client);
    }

    private static AWSStaticCredentialsProvider createAWSStaticCredentialsProvider(PluginTask task)
    {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(
                task.getAccessKeyId(),
                task.getSecretAccessKey()
        );

        return new AWSStaticCredentialsProvider(credentials);
    }
}
