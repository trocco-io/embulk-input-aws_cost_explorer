package org.embulk.input.aws_cost_explorer.client;

import com.amazonaws.services.costexplorer.AWSCostExplorer;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageRequest;
import com.amazonaws.services.costexplorer.model.GetCostAndUsageResult;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AwsCostExplorerClient
{
    private final AWSCostExplorer client;

    public AwsCostExplorerClient(AWSCostExplorer client)
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
            @Override
            public boolean tryAdvance(Consumer<? super AwsCostExplorerResponse> action)
            {
                final AwsCostExplorerResponse response = request(requestParameters);
                action.accept(response);

                final String nextPageToken = response.getNextPageToken();
                requestParameters.setNextPageToken(nextPageToken);

                return nextPageToken != null && !nextPageToken.isEmpty();
            }

            private AwsCostExplorerResponse request(GetCostAndUsageRequest requestParameters)
            {
                final GetCostAndUsageResult result = client.getCostAndUsage(requestParameters);
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
