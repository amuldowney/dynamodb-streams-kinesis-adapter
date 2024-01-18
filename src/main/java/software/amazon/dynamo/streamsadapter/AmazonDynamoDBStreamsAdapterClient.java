/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.dynamo.streamsadapter.model.AmazonServiceExceptionTransformer;
import software.amazon.dynamo.streamsadapter.model.DescribeStreamRequestMapper;
import software.amazon.dynamo.streamsadapter.model.DescribeStreamResponseMapper;
import software.amazon.dynamo.streamsadapter.model.GetRecordsRequestMapper;
import software.amazon.dynamo.streamsadapter.model.GetRecordsResponseMapper;
import software.amazon.dynamo.streamsadapter.model.GetShardIteratorRequestMapper;
import software.amazon.dynamo.streamsadapter.model.GetShardIteratorResultAdapter;
import software.amazon.dynamo.streamsadapter.model.ListStreamsRequestMapper;
import software.amazon.dynamo.streamsadapter.model.ListStreamsResponseMapper;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

/**
 * Client for accessing DynamoDB Streams using the Amazon Kinesis interface.
 */
public class AmazonDynamoDBStreamsAdapterClient implements KinesisAsyncClient {


    private static final int REQUEST_CACHE_CAPACITY = 50;

    private static final Log LOG = LogFactory.getLog(AmazonDynamoDBStreamsAdapterClient.class);

    private final DynamoDbStreamsClient internalClient;

    private static final String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";



    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record
         */
        SKIP_RECORDS_TO_TRIM_HORIZON, /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data
         */
        KCL_RETRY;
    }


    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;

    /**
     * Whether or not to generate the ByteBuffer returned by
     * RecordAdapter::getData().  KCL uses the bytes returned by getData to
     * generate throughput metrics.  If these metrics are not needed then
     * choosing to not generate this data results in memory and CPU savings.
     * If this value is true then the data will be generated and KCL will generate
     * a correct throughput metric.  If this is false, getData()
     * will return an empty ByteBuffer and the KCL metric will always be zero.
     */
    private boolean generateRecordBytes = true;

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     */
    public AmazonDynamoDBStreamsAdapterClient() {
        internalClient = DynamoDbStreamsClient.builder().build();
    }

    /**
     * Recommended constructor for AmazonDynamoDBStreamsAdapterClient which takes in an AmazonDynamoDBStreams
     * interface. If you need to execute setEndpoint(String,String,String) or setServiceNameIntern() methods,
     * you should do that on DynamoDbStreamsClient implementation before passing it in this constructor.
     *
     * @param amazonDynamoDBStreams The DynamoDB Streams to be used internally
     */
    public AmazonDynamoDBStreamsAdapterClient(DynamoDbStreamsClient amazonDynamoDBStreams) {
        internalClient = amazonDynamoDBStreams;
    }

    @Override public String serviceName() {
        return internalClient.serviceName();
    }

    @Override public void close() {
        internalClient.close();
    }

    @Override
   public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(
        DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        return CompletableFuture.supplyAsync(() -> {
            DescribeStreamRequest ddbDescribeStreamRequest = DescribeStreamRequestMapper.convert(describeStreamSummaryRequest);

            DescribeStreamResponse result;
            try {
                result = internalClient.describeStream(ddbDescribeStreamRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(
                    e);
            }
            return DescribeStreamResponseMapper.convertToSummary(result);
        });
    }

    /**
     * @param describeStreamRequest Container for the necessary parameters to execute the DescribeStream service method on DynamoDB
     *                              Streams.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse> describeStream(
        software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest describeStreamRequest)
        throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest
                ddbDescribeStreamRequest =
                DescribeStreamRequestMapper.convert(describeStreamRequest);

            DescribeStreamResponse result;
            try {
                result = internalClient.describeStream(ddbDescribeStreamRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(
                    e);
            }
            if (result.streamDescription().streamStatus().equals(StreamStatus.DISABLED)) {
                // FIXME short-term solution
                // KCL does not currently support the concept of disabled streams. If there
                // are no active shards (i.e. EndingSequenceNumber not null), then KCL will
                // not create leases and will not process any shards in that stream. As a
                // short-term solution, we feign active shards by setting the ending
                // sequence number of all leaf nodes in the shard tree to null.
                List<Shard> allShards = getAllShardsForDisabledStream(result);
                allShards = markLeafShardsAsActive(allShards);
                StreamDescription newStreamDescription =
                    StreamDescription.builder()
                        .shards(allShards)
                        .lastEvaluatedShardId(null)
                        .creationRequestDateTime(
                            result.streamDescription().creationRequestDateTime())
                        .keySchema(result.streamDescription().keySchema())
                        .streamArn(result.streamDescription().streamArn())
                        .streamLabel(result.streamDescription().streamLabel())
                        .streamStatus(result.streamDescription().streamStatus())
                        .tableName(result.streamDescription().tableName())
                        .streamViewType(result.streamDescription().streamViewType())
                        .build();

                result = DescribeStreamResponse.builder()
                    .streamDescription(newStreamDescription)
                    .build();
            }
            return DescribeStreamResponseMapper.convert(result);
        });
    }

    /**
     * @param getRecordsRequest Container for the necessary parameters to execute the GetRecords service method on DynamoDB Streams.
     * @return The response from the GetRecords service method, adapted for use with the AmazonKinesis model.
     */
    @Override public CompletableFuture<GetRecordsResponse> getRecords(
        software.amazon.awssdk.services.kinesis.model.GetRecordsRequest getRecordsRequest)
        throws AwsServiceException, SdkClientException{
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest requestAdapter =
                GetRecordsRequestMapper.convert(getRecordsRequest);

            try {
                software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse result =
                    internalClient.getRecords(requestAdapter);
                //List<Record> records = result.records();
                //if (records != null && !records.isEmpty()) {
                //    final int lastIndex = result.records().size() - 1;
                //    final StreamRecord lastStreamRecord =
                //        result.records().get(lastIndex).dynamodb();
                //    final double lastApproximateCreationTimestamp =
                //        lastStreamRecord.approximateCreationDateTime().toEpochMilli();
                //    final double millisBehindLatest =
                //        Math.max(System.currentTimeMillis() - lastApproximateCreationTimestamp, 0);
                //    //IMetricsScope scope = MetricsHelper.getMetricsScope();
                //    //scope.addData(MILLIS_BEHIND_LATEST_METRIC, millisBehindLatest,
                //    //    StandardUnit.Milliseconds, MetricsLevel.SUMMARY);
                //}
                return GetRecordsResponseMapper.convert(result, generateRecordBytes);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(
                    e, skipRecordsBehavior);
            }
        });
    }

    /**
     * @param getShardIteratorRequest Container for the necessary parameters to execute the GetShardIterator service method on DynamoDB
     *                                Streams.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override public CompletableFuture<GetShardIteratorResponse> getShardIterator(
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest getShardIteratorRequest)
        throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() ->  shardIterators(getShardIteratorRequest));
    }

    private GetShardIteratorResponse shardIterators(
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest getShardIteratorRequest)
        throws AwsServiceException, SdkClientException {
        GetShardIteratorRequest adaptedRequest = GetShardIteratorRequestMapper.convert(getShardIteratorRequest);

        try {
            software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse result = internalClient.getShardIterator(adaptedRequest);
            //if (result != null && result.shardIterator() == null && result.sdkFields() != null) {
            //    LOG.info("RequestId for getShardIterator call which resulted in ShardEnd: " + result.getSdkResponseMetadata().getRequestId());
            //}
            return GetShardIteratorResultAdapter.convert(result);
        } catch (TrimmedDataAccessException e) {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                if (getShardIteratorRequest.shardIteratorType().equals(
                    software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON)) {
                    throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
                }
                LOG.warn(String.format("Data has been trimmed. Intercepting DynamoDB exception and retrieving a fresh iterator %s", getShardIteratorRequest), e);
                return shardIterators(
                    getShardIteratorRequest.toBuilder()
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .startingSequenceNumber(null)
                        .build());
            } else {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
            }
        } catch (AwsServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
        }
    }

    /**
    //     * @param listStreamsRequest Container for the necessary parameters to execute the ListStreams service method on DynamoDB Streams.
    //     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
    //     */
    @Override public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest)
        throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest requestAdapter =
                ListStreamsRequestMapper.convert(listStreamsRequest);
            try {
                software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse result =
                    internalClient.listStreams(requestAdapter);
                return ListStreamsResponseMapper.convert(result);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisListStreams(
                    e);
            }
        });
    }

    /**
     * Determines RecordAdapter behavior when RecordAdapter::getData() is called.  This method should
     * usually be called immediately after instantiating this client.  If this method is called from
     * one thread while a GetRecords call is in progress on another thread then the behavior will be
     * non-deterministic.
     *
     * @param generateRecordBytes Whether or not to generate the ByteBuffer returned by
     *                            RecordAdapter::getData().  KCL uses the bytes returned by getData to
     *                            generate throughput metrics.  If these metrics are not needed then
     *                            choosing to not generate this data results in memory and CPU savings.
     *                            If this value is true then the data will be generated and KCL will generate
     *                            a correct throughput metric.  If this is false, getData()
     *                            will return an empty ByteBuffer and the KCL metric will always be zero.
     */
    public void setGenerateRecordBytes(boolean generateRecordBytes) {
        this.generateRecordBytes = generateRecordBytes;
    }


    private List<Shard> getAllShardsForDisabledStream(DescribeStreamResponse initialResult) {
      List<Shard> shards = new ArrayList<>(initialResult.streamDescription().shards());

        DescribeStreamRequest request;
        DescribeStreamResponse result = initialResult;
        // Allowing KCL to paginate calls will not allow us to correctly determine the
        // leaf nodes. In order to avoid pagination issues when feigning shard activity, we collect all
        // shards in the adapter and return them at once.
        while (result.streamDescription().lastEvaluatedShardId() != null) {
            request = DescribeStreamRequest.builder().streamArn(result.streamDescription().streamArn())
                .exclusiveStartShardId(result.streamDescription().lastEvaluatedShardId()).build();
            try {
                result = internalClient.describeStream(request);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(e);
            }
            shards.addAll(result.streamDescription().shards());
        }
        return shards;
    }

    private List<Shard> markLeafShardsAsActive(List<Shard> shards) {
        List<String> parentShardIds = new ArrayList<String>();
        for (Shard shard : shards) {
            if (shard.parentShardId() != null) {
                parentShardIds.add(shard.parentShardId());
            }
        }
        //Fake activity for leaf nodes
        return shards.stream().filter(shard -> !parentShardIds.contains(shard.shardId()))
            .map(shard -> shard.toBuilder().sequenceNumberRange(shard.sequenceNumberRange().toBuilder().endingSequenceNumber(null).build()).build())
            .collect(Collectors.toList());
    }


    /**
     * Gets the value of {@link SkipRecordsBehavior}.
     *
     * @return The value of {@link SkipRecordsBehavior}
     */
    public SkipRecordsBehavior getSkipRecordsBehavior() {
        return skipRecordsBehavior;
    }

    /**
     * Sets a value of {@link SkipRecordsBehavior} to decide how the application handles the case when records are lost.
     * Default = {@link SkipRecordsBehavior#SKIP_RECORDS_TO_TRIM_HORIZON}
     *
     * @param skipRecordsBehavior A {@link SkipRecordsBehavior} for the adapter
     */
    public void setSkipRecordsBehavior(SkipRecordsBehavior skipRecordsBehavior) {
        //TODO did we use this more extensively before? its only used in error messages?
        if (skipRecordsBehavior == null) {
            throw new NullPointerException("skipRecordsBehavior cannot be null");
        }
        this.skipRecordsBehavior = skipRecordsBehavior;
    }
}