package org.embulk.input.aws_cost_explorer.client;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.GetCostAndUsageRequest;
import software.amazon.awssdk.services.costexplorer.model.GetCostAndUsageResponse;

public class AwsCostExplorerClient
{
    private final CostExplorerClient client;

    public AwsCostExplorerClient(CostExplorerClient client)
    {
        this.client = client;
    }

    /**
     * Request all pages of the cost and usage data.
     *
     * @param requestParameters The request parameters for the cost and usage data.
     * @return A stream of the cost and usage data.
     */
    public Stream<AwsCostExplorerResponse> requestAll(GetCostAndUsageRequest requestParameters)
    {
        return StreamSupport.stream(makeResponseIterator(requestParameters), false);
    }

    private Spliterator<AwsCostExplorerResponse> makeResponseIterator(GetCostAndUsageRequest requestParameters)
    {
        return new Spliterator<AwsCostExplorerResponse>()
        {
            private String nextPageToken = null;
            private boolean isFirstRequest = true;

            @Override
            public boolean tryAdvance(Consumer<? super AwsCostExplorerResponse> action)
            {
                final GetCostAndUsageRequest.Builder requestBuilder = requestParameters.toBuilder();
                
                if (!isFirstRequest && nextPageToken != null) {
                    requestBuilder.nextPageToken(nextPageToken);
                }
                
                final AwsCostExplorerResponse response = request(requestBuilder.build());
                action.accept(response);

                nextPageToken = response.getNextPageToken();
                isFirstRequest = false;

                return nextPageToken != null && !nextPageToken.isEmpty();
            }

            private AwsCostExplorerResponse request(GetCostAndUsageRequest requestParameters)
            {
                final GetCostAndUsageResponse result = client.getCostAndUsage(requestParameters);
                return new AwsCostExplorerResponse(result);
            }

            @Override
            public Spliterator<AwsCostExplorerResponse> trySplit()
            {
                return null; // Parallel processing is not supported.
            }

            @Override
            public long estimateSize()
            {
                return Long.MAX_VALUE; // Size is unknown in advance.
            }

            @Override
            public int characteristics()
            {
                return Spliterator.NONNULL | Spliterator.IMMUTABLE;
            }
        };
    }
}
