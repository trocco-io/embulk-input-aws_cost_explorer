package org.embulk.input.aws_cost_explorer.client;

import org.embulk.input.aws_cost_explorer.PluginTask;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;

public class AwsCostExplorerClientFactory
{
    private static final Region REGION = Region.US_EAST_1;

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
        final CostExplorerClient client = CostExplorerClient.builder()
                .region(REGION)
                .credentialsProvider(createStaticCredentialsProvider(task))
                .build();

        return new AwsCostExplorerClient(client);
    }

    private static StaticCredentialsProvider createStaticCredentialsProvider(PluginTask task)
    {
        final AwsBasicCredentials credentials = AwsBasicCredentials.create(
                task.getAccessKeyId(),
                task.getSecretAccessKey()
        );

        return StaticCredentialsProvider.create(credentials);
    }
}
