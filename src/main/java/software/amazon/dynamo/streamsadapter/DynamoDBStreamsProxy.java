/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import software.amazon.dynamo.streamsadapter.model.ShardMapper;
import software.amazon.dynamo.streamsadapter.utils.Sleeper;
import software.amazon.dynamo.streamsadapter.utils.ThreadSleeper;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;


import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardDetector;

/**
 * An implementation of ShardDetector to make calls to DynamoDBStreams service.
 */
public class DynamoDBStreamsProxy implements ShardDetector {
    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsProxy.class);
    private static final long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
    private static final int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;

    /**
     * This constant is used to set the END SEQUENCE NUMBER in non-leaf nodes that are seen as open
     * due to aggregation of paginated shard lists of DescribeStream.
     */
    static final String END_SEQUENCE_NUMBER_TO_CLOSE_OPEN_PARENT = String.valueOf(Long.MAX_VALUE);

    /**
     * If jitter is not enabled, the default combination of DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS,
     * DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS and,
     * DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES is such that retries for fixing
     * shard graph occur for a little more than 60 seconds. The sequence obtained for 8 retries starting from 0 and
     * ending at 7 is {1400, 1600, 2000, 2800, 4400, 7600, 14000, 26800}, the cumulative sum sequence, or total
     * duration sums for which is {1400, 3000, 5000, 7800, 12200, 19800, 33800, 60600}.
     */
    private static final boolean DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED = true;
    private static final int DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES = 8;
    private static final long DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS = 200L; // Multiplier for exponential back-off
    private static final long DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS = 1200L; // Base for exponential back-off
    private static final long MAX_SHARD_COUNT_TO_TRIGGER_RETRIES = 1500L;

    private final KinesisAsyncClient client;
    private final StreamIdentifier streamIdentifier;

    private final AtomicReference<List<software.amazon.awssdk.services.kinesis.model.Shard>> listOfShardsSinceLastGet = new AtomicReference<>();
    private final Random random;

    private final boolean isInconsistencyResolutionRetryBackoffJitterEnabled;
    private final long describeStreamBackoffTimeInMillis;
    private final int maxDescribeStreamRetryAttempts;
    private final int maxRetriesToResolveInconsistencies;
    private final long inconsistencyResolutionRetryBackoffMultiplierInMillis;
    private final long inconsistencyResolutionRetryBackoffBaseInMillis;
    private final Sleeper sleeper;
    private ShardGraph shardGraph;

    /**
     *
     * @param streamIdentifier Data records will be fetched from this stream.
     * @param kinesisClient Kinesis client (used to fetch data from Kinesis).
     * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds.
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls.
     * @param maxRetriesToResolveInconsistencies Number of retry attempts to resolve any shard lineage inconsistencies.
     * @param inconsistencyResolutionRetryBackoffBaseInMillis Base for calculating backoff when resolving shard lineage
     *                                                        inconsistencies.
     * @param inconsistencyResolutionRetryBackoffMultiplierInMillis Multiplier for calculating backoff when resolving
     *                                                              shard lineage inconsistencies.
     * @param sleeper Simple abstraction on Thread.sleep to allow unit testing of backoff mechanism
     */
    private DynamoDBStreamsProxy(StreamIdentifier streamIdentifier,
        KinesisAsyncClient kinesisClient,
        long describeStreamBackoffTimeInMillis,
        int maxDescribeStreamRetryAttempts,
        int maxRetriesToResolveInconsistencies,
        long inconsistencyResolutionRetryBackoffBaseInMillis,
        long inconsistencyResolutionRetryBackoffMultiplierInMillis,
        boolean isDefaultInconsistencyResolutionRetryBackoffJitterEnabled,
        Sleeper sleeper,
        Random random) {
        this.streamIdentifier = streamIdentifier;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
        this.maxRetriesToResolveInconsistencies = maxRetriesToResolveInconsistencies;
        this.inconsistencyResolutionRetryBackoffBaseInMillis = inconsistencyResolutionRetryBackoffBaseInMillis;
        this.inconsistencyResolutionRetryBackoffMultiplierInMillis
            = inconsistencyResolutionRetryBackoffMultiplierInMillis;
        this.isInconsistencyResolutionRetryBackoffJitterEnabled
            = isDefaultInconsistencyResolutionRetryBackoffJitterEnabled;
        this.client = kinesisClient;
        this.sleeper = sleeper;
        this.random = random;

        LOG.debug("DynamoDBStreamsProxy( " + streamIdentifier.streamName() + ")");
    }


    @Override public software.amazon.awssdk.services.kinesis.model.Shard shard(String shardId) {
        if (this.listOfShardsSinceLastGet.get() == null) {
            //Update this.listOfShardsSinceLastGet as needed.
            this.listShards();
        }

        for (software.amazon.awssdk.services.kinesis.model.Shard shard : listOfShardsSinceLastGet.get()) {
            if (shard.shardId().equals(shardId))  {
                return shard;
            }
        }

        LOG.warn("Cannot find the shard given the shardId " + shardId);
        return null;
    }

    public GetRecordsResponse get(String shardIterator, int maxRecords) {
        return client.getRecords(
            GetRecordsRequest.builder().shardIterator(shardIterator).limit(maxRecords).build()).join();
        //
        //final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        //getRecordsRequest.setRequestCredentials(credentialsProvider.getCredentials());
        //getRecordsRequest.setShardIterator(shardIterator);
        //getRecordsRequest.setLimit(maxRecords);
        //final GetRecordsResult response = client.getRecords(getRecordsRequest);
        //return response;
    }


    public DescribeStreamResponse getStreamInfo(String startShardId) {
        final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
            .streamName(streamIdentifier.streamName())
            .exclusiveStartShardId(startShardId)
            .build();;
        DescribeStreamResponse response = null;

        LimitExceededException lastException = null;

        int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
        // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
        while (response == null) {
            try {
                response = client.describeStream(describeStreamRequest).join();
            } catch (LimitExceededException le) {
                LOG.info("Got LimitExceededException when describing stream " + streamIdentifier.streamName() + ". Backing off for "
                    + this.describeStreamBackoffTimeInMillis + " millis.");
                sleeper.sleep(this.describeStreamBackoffTimeInMillis);
                lastException = le;
            }
            remainingRetryTimes--;
            if (remainingRetryTimes == 0 && response == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from DescribeStream call.");
            }
        }

        final String streamStatus = response.streamDescription().streamStatusAsString();
        if (StreamStatus.ACTIVE.toString().equals(streamStatus)
            || StreamStatus.UPDATING.toString().equals(streamStatus)) {
            return response;
        } else {
            LOG.info("Stream is in status " + streamStatus
                + ", DescribeStream returning null (wait until stream is Active or Updating");
            return null;
        }
    }

    @Override
    public StreamIdentifier streamIdentifier() {
        return this.streamIdentifier;
    }
    @Override public List<software.amazon.awssdk.services.kinesis.model.Shard> listShards() {
        return listShardsInternal(false);
    }

    @Override public List<software.amazon.awssdk.services.kinesis.model.Shard> listShardsWithFilter(
        software.amazon.awssdk.services.kinesis.model.ShardFilter shardFilter) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support Shard List Filtering");
    }

    @Override
    public List<software.amazon.awssdk.services.kinesis.model.Shard> listShardsWithoutConsumingResourceNotFoundException() {
        return listShardsInternal( true);
    }



    private List<software.amazon.awssdk.services.kinesis.model.Shard> listShardsInternal(boolean shouldPropagateResourceNotFoundException) {
        //todo deal with shouldPropagateResourceNotFoundException
        if (shardGraph == null) {
            shardGraph = new ShardGraph();
        }

        // ShardGraph may not be empty if this call is being made after DescribeStream throttling.
        // In that case, the graph will have a lot of closed leaf nodes since their descendants were not
        // discovered earlier due to throttling. We do not handle that explicitly and allow the next round of
        // inconsistency fix attempts to resolve it.
        if (buildShardGraphSnapshot() == ShardGraphProcessingResult.STREAM_DISABLED) {
            LOG.info("Stream was disabled during getShardList operation.");
            return null;
        }

        if (shardGraph.size() < MAX_SHARD_COUNT_TO_TRIGGER_RETRIES) {
            int retryAttempt = 0;
            while (shardGraph.closedLeafNodeCount() > 0 && retryAttempt < maxRetriesToResolveInconsistencies) {
                final long backOffTime = getInconsistencyBackoffTimeInMillis(retryAttempt);
                String infoMsg = String.format("Inconsistency resolution retry attempt: %d. Backing off for %d millis.",
                    retryAttempt, backOffTime);
                LOG.info(infoMsg);
                sleeper.sleep(backOffTime);
                ShardGraphProcessingResult shardGraphProcessingResult = resolveInconsistenciesInShardGraph();
                if (shardGraphProcessingResult.equals(ShardGraphProcessingResult.STREAM_DISABLED)) {
                    LOG.info("Stream was disabled during getShardList operation.");
                    return null;
                } else if (shardGraphProcessingResult.equals(ShardGraphProcessingResult.RESOLVED_INCONSISTENCIES_AND_ABORTED)) {
                    infoMsg = String.format("An intermediate page in DescribeStream response resolved inconsistencies. "
                        + "Total retry attempts taken to resolve inconsistencies: %d", retryAttempt + 1);
                    LOG.info(infoMsg);
                    break;
                }
                retryAttempt++;
            }
            if (retryAttempt == maxRetriesToResolveInconsistencies && shardGraph.closedLeafNodeCount() > 0) {
                LOG.warn("Inconsistencies in the shard graph were not resolved after exhausting all retries.");
            }
        } else {
            if (shardGraph.closedLeafNodeCount() > 0) {
                String msg = String.format("Returning shard list with %s closed leaf node shards.",
                    shardGraph.closedLeafNodeCount());
                LOG.debug(msg);
            }
        }

        this.listOfShardsSinceLastGet.set(shardGraph.getShards());
        this.shardGraph = new ShardGraph();
        return listOfShardsSinceLastGet.get();
    }

    //@Override
    //public synchronized List<Shard> getShardListWithFilter(ShardFilter shardFilter){
    //
    //    throw new UnsupportedOperationException("DynamoDB Streams does not support Shard List Filtering");
    //}
    //
    //@Override
    ///**
    // * This method gets invoked from ShutdownTask when the shard consumer is shutting down.
    // * Kinesis modified KCL to verify that the shard being closed has children, and this requires listing all shards.
    // * Since DynamoDB streams can have a large number of shards, this verification delays shard closure, and can severely
    // * degrade processing performance. For large streams, this can even cause stream processing to completely halt.
    // * Therefore, we skip performing this validation in ShutdownTask by simply returning true.
    // */
    //public ShardClosureVerificationResponse verifyShardClosure(String shardId) {
    //    return () -> true; // isShardClosed -> true
    //}

    private ShardGraphProcessingResult buildShardGraphSnapshot() {

        DescribeStreamResponse response;

        do {
            response = getStreamInfo(shardGraph.getLastFetchedShardId());
            if (response == null) {
                /*
                 * If getStreamInfo ever returns null, we should bail and return null from getShardList.
                 * This indicates the stream is not in ACTIVE state and we may not have accurate/consistent information
                 * about the stream. By returning ShardGraphProcessingResult.STREAM_DISABLED from here, we indicate that
                 * getStreamInfo returned a null response and the caller (getShardList) should return null. If, on the
                 * other hand, an exception is thrown from getStreamInfo, it will bubble up to the caller of
                 * getShardList, which then handles it accordingly.
                 */
                return ShardGraphProcessingResult.STREAM_DISABLED;
            } else {
                shardGraph.addNodes(response.streamDescription().shards());
                LOG.debug(String.format("Building shard graph snapshot; total shard count: %d", shardGraph.size()));
            }
        } while (response.streamDescription().hasMoreShards());
        return ShardGraphProcessingResult.FETCHED_ALL_AVAILABLE_SHARDS;
    }

    private ShardGraphProcessingResult resolveInconsistenciesInShardGraph() {
        DescribeStreamResponse response;
        final String warnMsg = String.format("Inconsistent shard graph state detected. "
            + "Fetched: %d shards. Closed leaves: %d shards", shardGraph.size(), shardGraph.closedLeafNodeCount());
        LOG.warn(warnMsg);
        if (LOG.isDebugEnabled()) {
            final String debugMsg = String.format("Following leaf node shards are closed: %s",
                String.join(", ", shardGraph.getAllClosedLeafNodeIds()));
            LOG.debug(debugMsg);
        }
        String exclusiveStartShardId = shardGraph.getEarliestClosedLeafNodeId();
        do {
            response = getStreamInfo(exclusiveStartShardId);
            if (response == null) {
                return ShardGraphProcessingResult.STREAM_DISABLED;
            } else {
                shardGraph.addToClosedLeafNodes(response.streamDescription().shards());
                LOG.debug(String.format("Resolving inconsistencies in shard graph; total shard count: %d",
                    shardGraph.size()));
                if (shardGraph.closedLeafNodeCount() == 0) {
                    return ShardGraphProcessingResult.RESOLVED_INCONSISTENCIES_AND_ABORTED;
                }
                exclusiveStartShardId = shardGraph.getLastFetchedShardId();
            }
        } while (response.streamDescription().hasMoreShards());
        return ShardGraphProcessingResult.FETCHED_ALL_AVAILABLE_SHARDS;
    }

    @VisibleForTesting
    long getInconsistencyBackoffTimeInMillis(int retryAttempt) {
        double baseMultiplier = isInconsistencyResolutionRetryBackoffJitterEnabled ? random.nextDouble() : 1.0;
        return (long)(baseMultiplier * inconsistencyResolutionRetryBackoffBaseInMillis) +
            (long)Math.pow(2.0, retryAttempt) * inconsistencyResolutionRetryBackoffMultiplierInMillis;
    }

    @Override public List<ChildShard> getChildShards(String shardId)
        throws InterruptedException, ExecutionException, TimeoutException {
        //todo impl
        return ShardDetector.super.getChildShards(shardId);
    }

    private enum ShardGraphProcessingResult {
        STREAM_DISABLED,
        FETCHED_ALL_AVAILABLE_SHARDS,
        RESOLVED_INCONSISTENCIES_AND_ABORTED
    }

    private static class ShardNode {

        private final software.amazon.awssdk.services.kinesis.model.Shard shard;

        private final Set<String> descendants;

        ShardNode(software.amazon.awssdk.services.kinesis.model.Shard shard) {
            this.shard = shard;
            descendants = new HashSet<>();
        }

        ShardNode(software.amazon.awssdk.services.kinesis.model.Shard shard, Set<String> descendants) {
            this.shard = shard;
            this.descendants = descendants;
        }

        public software.amazon.awssdk.services.kinesis.model.Shard getShard() {
            return shard;
        }

        Set<String> getDescendants() {
            return descendants;
        }

        public String getShardId() {
            return shard.shardId();
        }

        boolean isShardClosed() {
            return shard.sequenceNumberRange() != null &&
                shard.sequenceNumberRange().endingSequenceNumber() != null;
        }

        boolean addDescendant(String shardId) {
            return descendants.add(shardId);
        }
    }

    private static class ShardGraph {

        private final Map<String, ShardNode> nodes;

        private final TreeSet<String> closedLeafNodeIds;

        private String lastFetchedShardId;

        public ShardGraph() {
            nodes = new HashMap<>();
            closedLeafNodeIds = new TreeSet<>();
        }

        String getLastFetchedShardId() {
            return lastFetchedShardId;
        }

        String getEarliestClosedLeafNodeId() {
            if (closedLeafNodeIds.isEmpty()) {
                return null;
            } else {
                return closedLeafNodeIds.first();
            }
        }

        /**
         * Adds a list of shards to the graph.
         * @param shards List of shards to be added to the graph.
         */
        private void addNodes(List<software.amazon.awssdk.services.kinesis.model.Shard> shards) {
            if (null == shards) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Updating the graph with the following shards: \n %s",
                    String.join(", ", shards.stream().map(software.amazon.awssdk.services.kinesis.model.Shard::shardId).collect(Collectors.toList()))));
            }
            for (software.amazon.awssdk.services.kinesis.model.Shard shard : shards) {
                addNode(shard);
            }
            updateLastFetchedShardId(shards);
        }

        private ShardNode setShardEndSequenceNumberForOpenParent(ShardNode parentNode, ShardNode childNode) {
            software.amazon.awssdk.services.kinesis.model.Shard innerShard = parentNode.getShard();
            software.amazon.awssdk.services.kinesis.model.SequenceNumberRange innerSequenceNumberRange = parentNode.getShard().sequenceNumberRange();
            if (innerSequenceNumberRange != null && innerSequenceNumberRange.endingSequenceNumber() == null) {
                LOG.debug(String.format("Marked open parent shard %s of shard %s as closed",
                    parentNode.getShard().shardId(), childNode.getShard().shardId()));
                software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange modifiedSequenceNumberRange
                    = software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
                    .startingSequenceNumber(innerSequenceNumberRange.startingSequenceNumber())
                    .endingSequenceNumber(END_SEQUENCE_NUMBER_TO_CLOSE_OPEN_PARENT).build();
                software.amazon.awssdk.services.dynamodb.model.Shard shard
                    = software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                    .shardId(innerShard.shardId())
                    .parentShardId(innerShard.parentShardId())
                    .sequenceNumberRange(modifiedSequenceNumberRange).build();
                software.amazon.awssdk.services.kinesis.model.Shard shardAdapter = ShardMapper.convert(shard);//  new ShardAdapter(shard)
                ShardNode newParentNode = new ShardNode(shardAdapter, parentNode.getDescendants());
                // parentNode has been modified, overwrite corresponding entry in the map.
                nodes.put(newParentNode.getShardId(), newParentNode);
                return newParentNode;
            }
            return parentNode;
        }

        /**
         * Adds descendants only to closed leaf nodes in order to ensure all leaf nodes in
         * the graph are open.
         * @param shards list of shards obtained from DescribeStream call.
         */
        private void addToClosedLeafNodes(List<software.amazon.awssdk.services.kinesis.model.Shard> shards) {
            if (null == shards) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Attempting to resolve inconsistencies in the graph with the following shards: \n %s",
                    shards.stream().map(software.amazon.awssdk.services.kinesis.model.Shard::shardId)
                        .collect(Collectors.joining(", "))));
            }
            for (software.amazon.awssdk.services.kinesis.model.Shard shard : shards) {
                final String parentShardId = shard.parentShardId();
                if (null != parentShardId && closedLeafNodeIds.contains(parentShardId)) {
                    ShardNode shardNode = addNode(shard);
                    closedLeafNodeIds.remove(parentShardId);
                    if (shardNode.isShardClosed()) {
                        closedLeafNodeIds.add(shardNode.getShardId());
                    }
                }
            }
            updateLastFetchedShardId(shards);
        }

        private void updateLastFetchedShardId(List<software.amazon.awssdk.services.kinesis.model.Shard> shards) {
            if (!shards.isEmpty()) {
                software.amazon.awssdk.services.kinesis.model.Shard lastShard = shards.get(shards.size() - 1);
                lastFetchedShardId = lastShard.shardId();
            }
        }

        private ShardNode addNode(software.amazon.awssdk.services.kinesis.model.Shard shard) {
            final ShardNode shardNode = new ShardNode(shard);
            nodes.put(shardNode.getShardId(), shardNode);
            // if the node is closed, add it to the closed leaf node set.
            // once its child appears, this node will be removed from the set.
            if (shardNode.isShardClosed()) {
                closedLeafNodeIds.add(shardNode.getShardId());
            }
            final String parentShardID = shard.parentShardId();
            // Ensure nodes contains the parent shard, since older shards are trimmed and we will see nodes whose
            // parent shards are not in the graph.
            if (null != parentShardID && nodes.containsKey(parentShardID)) {
                ShardNode parentNode = nodes.get(parentShardID);

                // If parent shard is still open, it's because of pagination in DescribeStream results.
                // We mark the parent shard as closed by setting the end sequence number to a fixed value.
                // This ensures we do not return any parent-open-child-open type of inconsistencies in shard list.
                parentNode = setShardEndSequenceNumberForOpenParent(parentNode, shardNode);
                parentNode.addDescendant(shard.shardId());
                closedLeafNodeIds.remove(parentShardID);
            }
            return shardNode;
        }

        private int size() {
            return nodes.size();
        }

        private int closedLeafNodeCount() {
            return closedLeafNodeIds.size();
        }

        Set<String> getAllClosedLeafNodeIds() {
            return closedLeafNodeIds;
        }

        List<software.amazon.awssdk.services.kinesis.model.Shard> getShards() {
            return nodes.values().stream().map(ShardNode::getShard).collect(Collectors.toList());
        }
    }

    public static class Builder {

        private int maxDescribeStreamRetryAttempts = DEFAULT_DESCRIBE_STREAM_RETRY_TIMES;
        private int maxRetriesToResolveInconsistencies = DEFAULT_MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES;
        private long describeStreamBackoffTimeInMillis = DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS;
        private long inconsistencyResolutionRetryBackoffMultiplierInMillis = DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS;
        private long inconsistencyResolutionRetryBackoffBaseInMillis = DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS;
        private boolean isInconsistencyResolutionRetryBackoffJitterEnabled = DEFAULT_INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED;
        private final StreamIdentifier streamIdentifier;
        private final KinesisAsyncClient kinesisClient;
        private Sleeper sleeper;
        private Random random;

        public Builder(String streamName, KinesisAsyncClient kinesisClient) {
            this.kinesisClient = kinesisClient;
            this.streamIdentifier = StreamIdentifier.singleStreamInstance(streamName);
        }

        public Builder withMaxDescribeStreamRetryAttempts(int maxDescribeStreamRetryAttempts) {
            this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
            return this;
        }

        public Builder withMaxRetriesToResolveInconsistencies(int maxRetriesToResolveInconsistencies) {
            this.maxRetriesToResolveInconsistencies = maxRetriesToResolveInconsistencies;
            return this;
        }

        public Builder withDescribeStreamBackoffTimeInMillis(long describeStreamBackoffTimeInMillis) {
            this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
            return this;
        }

        public Builder withInconsistencyResolutionRetryBackoffMultiplierInMillis(
            long inconsistencyResolutionRetryBackoffMultiplierInMillis) {
            this.inconsistencyResolutionRetryBackoffMultiplierInMillis = inconsistencyResolutionRetryBackoffMultiplierInMillis;
            return this;
        }

        public Builder withInconsistencyResolutionRetryBackoffBaseInMillis(
            long inconsistencyResolutionRetryBackoffBaseInMillis) {
            this.inconsistencyResolutionRetryBackoffBaseInMillis = inconsistencyResolutionRetryBackoffBaseInMillis;
            return this;
        }

        public Builder withInconsistencyResolutionRetryBackoffJitterEnabled(
            boolean inconsistencyResolutionRetryBackoffJitterEnabled) {
            this.isInconsistencyResolutionRetryBackoffJitterEnabled = inconsistencyResolutionRetryBackoffJitterEnabled;
            return this;
        }

        public Builder withSleeper(Sleeper sleeper) {
            this.sleeper = sleeper;
            return this;
        }

        public Builder withRandomNumberGeneratorForJitter(Random randomNumberGeneratorForJitter) {
            this.random = randomNumberGeneratorForJitter;
            return this;
        }

        public DynamoDBStreamsProxy build() {
            if (null == sleeper) {
                sleeper = new ThreadSleeper();
            }
            if (null == random) {
                random = ThreadLocalRandom.current();
            }
            return new DynamoDBStreamsProxy(
                streamIdentifier,
                kinesisClient,
                describeStreamBackoffTimeInMillis,
                maxDescribeStreamRetryAttempts,
                maxRetriesToResolveInconsistencies,
                inconsistencyResolutionRetryBackoffBaseInMillis,
                inconsistencyResolutionRetryBackoffMultiplierInMillis,
                isInconsistencyResolutionRetryBackoffJitterEnabled,
                sleeper,
                random);
        }

    }
}