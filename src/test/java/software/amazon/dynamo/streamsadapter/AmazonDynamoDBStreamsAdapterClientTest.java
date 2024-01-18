/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import java.time.Instant;
import java.util.ArrayList;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamRequest;
import software.amazon.awssdk.services.kinesis.model.MergeShardsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamRequest;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.dynamo.streamsadapter.model.GetRecordsRequestMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import software.amazon.dynamo.streamsadapter.AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior;

@PrepareForTest({AmazonDynamoDBStreamsAdapterClient.class, DynamoDbStreamsClient.class})
@RunWith(PowerMockRunner.class)
public class AmazonDynamoDBStreamsAdapterClientTest {
    private static final int LIMIT = 50;

    private static final int KINESIS_GET_RECORDS_LIMIT = 10000;

    private static final String STREAM_0000000 = "0000000";

    private static final String SHARD_0000000 = "shard-0000000";

    private static final String SHARD_ITERATOR = "shard-iterator";

    private static final String SHARD_0000001 = "shard-0000001";

    private static final String STREAM_ID = "streamId";

    private static final String TEST_STRING = "TestString";

    private static final String DUMMY_ACCESS_KEY = "dummyAccessKey";

    private static final String DUMMY_SECRET_KEY = "dummySecretKey";

    private static final String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";


    private static final AwsCredentials
        CREDENTIALS = AwsBasicCredentials.create(DUMMY_ACCESS_KEY, DUMMY_SECRET_KEY);

    private static final AwsCredentialsProvider CREDENTIALS_PROVIDER = () -> CREDENTIALS;

    //private static final RequestMetricCollector REQUEST_METRIC_COLLECTOR = RequestMetricCollector.NONE;

    private static final Map<String, AttributeValue> KEYS = Collections.emptyMap();

    private static final StreamRecord STREAM_RECORD =
        StreamRecord.builder().keys(KEYS).sequenceNumber(TEST_STRING).sizeBytes(0L).streamViewType(
            StreamViewType.KEYS_ONLY).approximateCreationDateTime(
            Instant.now()).build();


    private static final Record RECORD =
        Record.builder().awsRegion(TEST_STRING).dynamodb(STREAM_RECORD).eventID(TEST_STRING).eventName(
                OperationType.INSERT).eventSource(TEST_STRING).eventVersion(TEST_STRING).build();

    private static final Stream STREAM = Stream.builder().streamArn(STREAM_ID).streamLabel(TEST_STRING).tableName(TEST_STRING).build();

    private static final SequenceNumberRange SEQ_NUMBER_RANGE = SequenceNumberRange.builder().startingSequenceNumber("0").endingSequenceNumber("1").build();

    private static final Shard SHARD = Shard.builder().shardId(TEST_STRING).sequenceNumberRange(SEQ_NUMBER_RANGE).build();

    private static final StreamDescription STREAM_DESCRIPTION =
        StreamDescription.builder().creationRequestDateTime(Instant.now()).shards(SHARD).streamArn(TEST_STRING).streamStatus(StreamStatus.ENABLED)
            .streamViewType(StreamViewType.KEYS_ONLY).tableName(TEST_STRING).build();

    private static final DescribeStreamResponse DESCRIBE_STREAM_RESULT = DescribeStreamResponse.builder().streamDescription(STREAM_DESCRIPTION).build();

    private static final String SERVICE_NAME = "dynamodb";

    private static final String REGION_ID = "us-east-1";

    private DynamoDbStreamsClient mockClient;

    private AmazonDynamoDBStreamsAdapterClient adapterClient;

    @Before
    public void setUpTest() {
        mockClient = mock(DynamoDbStreamsClient.class);
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(mockClient);
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(DESCRIBE_STREAM_RESULT);
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
            .thenReturn(software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse.builder().build());
        when(mockClient.getRecords(any(GetRecordsRequest.class)))
            .thenReturn(GetRecordsResponse.builder().records(new ArrayList<>()).build());
        when(mockClient.listStreams(any(ListStreamsRequest.class)))
            .thenReturn(ListStreamsResponse.builder().build());
    }

    //@Test
    //public void testConstructorEmpty() throws Exception {
    //    when(DynamoDbStreamsClient.create()).thenReturn(mockClient);
    //    //whenNew(DynamoDbStreamsClient.class).withNoArguments().thenReturn(mockClient);
    //    new AmazonDynamoDBStreamsAdapterClient();
    //    verifyNew(DynamoDbStreamsClient.class).withNoArguments();
    //}

    //TODO IMPL
    //@Test
    //public void testConstructorStreamsClient() throws Exception {
    //    whenNew(DynamoDbStreamsClient.class).withParameterTypes(AwsCredentials.class).withArguments(isA(AwsCredentials.class))
    //        .then(new Answer<DynamoDbStreamsClient>() {
    //
    //            @Override
    //            public DynamoDbStreamsClient answer(InvocationOnMock invocation) throws Throwable {
    //              AwsCredentials credentials = invocation.getArgumentAt(0, AwsCredentials.class);
    //                assertEquals(DUMMY_ACCESS_KEY, credentials.accessKeyId());
    //                assertEquals(DUMMY_SECRET_KEY, credentials.secretAccessKey());
    //                return mockClient;
    //            }
    //        });
    //    new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS);
    //    verifyNew(DynamoDbStreamsClient.class).withArguments(isA(AwsCredentials.class));
    //}

    @Test
    public void testDescribeStream() {
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(DESCRIBE_STREAM_RESULT);
        software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse result = adapterClient.describeStream(
            software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder().streamName(STREAM_ID).build()).join();
      software.amazon.awssdk.services.kinesis.model.StreamDescription actual = result.streamDescription();
        StreamDescription expected = DESCRIBE_STREAM_RESULT.streamDescription();
        assertEquals(expected.streamArn(), actual.streamARN());
        assertEquals(expected.shards().size(), actual.shards().size());
        verify(mockClient).describeStream(any(DescribeStreamRequest.class));
    }

    @Test
    public void testDescribeStream2() {
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenAnswer(
            (Answer<DescribeStreamResponse>) invocation -> {
                final DescribeStreamRequest request =
                    invocation.getArgumentAt(0, DescribeStreamRequest.class);
                assertEquals(STREAM_0000000, request.streamArn());
                assertEquals(SHARD_0000000, request.exclusiveStartShardId());
                return DESCRIBE_STREAM_RESULT;
            });
      software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest request = software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder().streamName(STREAM_0000000).exclusiveStartShardId(SHARD_0000000).build();
      software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse result = adapterClient.describeStream(request).join();
      software.amazon.awssdk.services.kinesis.model.StreamDescription actual = result.streamDescription();
        StreamDescription expected = DESCRIBE_STREAM_RESULT.streamDescription();
        assertEquals(expected.streamArn(), actual.streamARN());
        assertEquals(expected.shards().size(), actual.shards().size());
        verify(mockClient).describeStream(any(DescribeStreamRequest.class));
    }

    @Test
    public void testGetShardIterator() {
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class)))
            .thenReturn(software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse.builder().shardIterator(TEST_STRING).build());
        adapterClient.getShardIterator(software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder().streamName(STREAM_ID).shardId(SHARD_0000001).startingSequenceNumber("123").shardIteratorType(SHARD_ITERATOR).build()).join();
        verify(mockClient).getShardIterator(any(GetShardIteratorRequest.class));
    }

    @Test
    public void testGetShardIterator2() {
        final String shardIteratorType = ShardIteratorType.TRIM_HORIZON.toString();
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class))).thenAnswer(
            (Answer<software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse>) invocation -> {
               GetShardIteratorRequest request =
                    invocation.getArgumentAt(0, GetShardIteratorRequest.class);
                assertEquals(STREAM_ID, request.streamArn());
                assertEquals(SHARD_0000001, request.shardId());
                assertEquals(shardIteratorType, request.shardIteratorTypeAsString());
                return software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse.builder().shardIterator(SHARD_ITERATOR).build();
            });
        assertEquals(SHARD_ITERATOR, adapterClient.getShardIterator(
            software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder().streamName(STREAM_ID).shardId(SHARD_0000001).startingSequenceNumber("123").shardIteratorType(shardIteratorType).build()).join().shardIterator());

        verify(mockClient).getShardIterator(any(GetShardIteratorRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecord() {
        adapterClient.putRecord(PutRecordRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecords() {
        adapterClient.putRecords(PutRecordsRequest.builder().build());
    }

    @Test
    public void testGetRecords() {
        when(mockClient.getRecords(any(GetRecordsRequest.class))).thenReturn(GetRecordsResponse.builder().records(RECORD).build());
      software.amazon.awssdk.services.kinesis.model.GetRecordsResponse result = adapterClient.getRecords(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder().shardIterator(TEST_STRING).build()).join();
        assertEquals(1, result.records().size());
        verify(mockClient).getRecords(any(GetRecordsRequest.class));
    }

    @Test
    public void testGetRecordsLimit() {
        when(mockClient.getRecords(any(GetRecordsRequest.class))).then(
            (Answer<GetRecordsResponse>) invocation -> {
                GetRecordsRequest request = invocation.getArgumentAt(0, GetRecordsRequest.class);
                assertEquals(LIMIT, request.limit().intValue());
                assertEquals(SHARD_ITERATOR, request.shardIterator());
                return GetRecordsResponse.builder().records(RECORD).build();
            });
      software.amazon.awssdk.services.kinesis.model.GetRecordsResponse result = adapterClient.getRecords(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder().shardIterator(SHARD_ITERATOR).limit(LIMIT).build()).join();
        assertEquals(1, result.records().size());
        verify(mockClient).getRecords(any(GetRecordsRequest.class));
    }

    @Test
    public void testGetRecordsLimitExceedDynamoDBStreams() {
        when(mockClient.getRecords(any(GetRecordsRequest.class))).then(
            (Answer<GetRecordsResponse>) invocation -> {
                GetRecordsRequest request = invocation.getArgumentAt(0, GetRecordsRequest.class);
                assertEquals(1000, request.limit().intValue());
                assertEquals(SHARD_ITERATOR, request.shardIterator());
                return GetRecordsResponse.builder().records(RECORD).build();
            });
      software.amazon.awssdk.services.kinesis.model.GetRecordsResponse result =
            adapterClient.getRecords(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder().shardIterator(SHARD_ITERATOR).limit(KINESIS_GET_RECORDS_LIMIT).build()).join();
        assertEquals(1, result.records().size());
        verify(mockClient).getRecords(any(GetRecordsRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSplitShard() {
        adapterClient.splitShard(SplitShardRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateStream() {
        adapterClient.createStream(CreateStreamRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteStream() {
        adapterClient.deleteStream(DeleteStreamRequest.builder().build());
    }

    @Test
    public void testListStreams() {
        final ListStreamsResponse result = ListStreamsResponse.builder().streams(STREAM).build();
        when(mockClient.listStreams(any(ListStreamsRequest.class))).thenReturn(result);
      adapterClient.listStreams(software.amazon.awssdk.services.kinesis.model.ListStreamsRequest.builder().exclusiveStartStreamName(STREAM_0000000).build()).join();
      verify(mockClient).listStreams(any(ListStreamsRequest.class));
    }

    @Test
    public void testListStreams2() {
        when(mockClient.listStreams(any(ListStreamsRequest.class))).then(
            (Answer<ListStreamsResponse>) invocation -> {
                ListStreamsRequest request = invocation.getArgumentAt(0, ListStreamsRequest.class);
                assertEquals(STREAM_0000000, request.exclusiveStartStreamArn());
                return ListStreamsResponse.builder().streams(STREAM).build();
            });
       software.amazon.awssdk.services.kinesis.model.ListStreamsResponse result = adapterClient.listStreams(software.amazon.awssdk.services.kinesis.model.ListStreamsRequest.builder().exclusiveStartStreamName(STREAM_0000000).build()).join();
        assertEquals(1, result.streamNames().size());
        assertEquals(STREAM.streamArn(), result.streamNames().get(0));
        verify(mockClient).listStreams(any(ListStreamsRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeShards() {
        adapterClient.mergeShards(MergeShardsRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddTagsToStream() {
        adapterClient.addTagsToStream(AddTagsToStreamRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListTagsForStream() {
        adapterClient.listTagsForStream(ListTagsForStreamRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveTagsFromStream() {
        adapterClient.removeTagsFromStream(RemoveTagsFromStreamRequest.builder().build());
    }

    @Test(expected = NullPointerException.class)
    public void testSetSkipRecordsBehaviorNull() {
        adapterClient.setSkipRecordsBehavior(null);
    }

    @Test
    public void testDefaultSkipRecordsBehavior() {
        assertEquals(SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, adapterClient.getSkipRecordsBehavior());
    }

    @Test
    public void testShutdown() {
        adapterClient.close();
        verify(mockClient).close();
    }

    //@Test
    //public void testMillisBehindNowPublished() {
    //
    //    final StreamRecord streamRecord =
    //            StreamRecord.builder().keys(KEYS).sequenceNumber(TEST_STRING).sizeBytes(0L).streamViewType(StreamViewType.KEYS_ONLY).approximateCreationDateTime(new Date(System.currentTimeMillis() - 250).toInstant()).build();
    //
    //    final Record record =
    //            Record.builder().awsRegion(TEST_STRING).dynamodb(streamRecord).eventID(TEST_STRING).eventName(OperationType.INSERT).eventSource(TEST_STRING)
    //                        .eventVersion(TEST_STRING).build();
    //
    //    final double maxMillisBehindLatest = 500.0;
    //    final double minMillisBehindLatest = 250.0;
    //
    //    Record record1 = spy(record);
    //    Record record2 = spy(record);
    //
    //    when(mockClient.getRecords(any(GetRecordsRequest.class))).thenReturn(GetRecordsResponse.builder().records(Arrays.asList(record1, record2)).build());
    //    IMetricsScope mockScope = mock(CWMetricsScope.class);
    //    MetricsHelper.setMetricsScope(mockScope);
    //    adapterClient.getRecords(GetRecordsRequest.builder().withShardIterator(TEST_STRING));
    //    verify(record1, times(0)).getDynamodb();
    //    verify(record2, times(1)).getDynamodb();
    //
    //    ArgumentCaptor<Double> argument = ArgumentCaptor.forClass(Double.class);
    //    verify(mockScope, times(1)).addData(eq(MILLIS_BEHIND_LATEST_METRIC), argument.capture(), eq(StandardUnit.Milliseconds), eq(MetricsLevel.SUMMARY));
    //    //Verifying the order of magnitude of MillisBehindNow falls within the range
    //    assertTrue(argument.getValue() < maxMillisBehindLatest && argument.getValue() >= minMillisBehindLatest);
    //    MetricsHelper.unsetMetricsScope();
    //}
    //
    //@Test
    //public void testMillisBehindNowWithoutRecords() {
    //    when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).thenReturn(GetRecordsResponse.builder());
    //    IMetricsScope mockScope = mock(CWMetricsScope.class);
    //    MetricsHelper.setMetricsScope(mockScope);
    //    adapterClient.getRecords(GetRecordsRequest.builder().withShardIterator(TEST_STRING));
    //    verify(mockScope, times(0)).addData(eq(MILLIS_BEHIND_LATEST_METRIC), anyDouble(), eq(StandardUnit.Milliseconds), eq(MetricsLevel.SUMMARY));
    //    MetricsHelper.unsetMetricsScope();
    //}
    //
    //@Test
    //public void testMillisBehindNowWithEmptyRecordsList() {
    //
    //    when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).thenReturn(GetRecordsResponse.builder().withRecords(Arrays.asList()));
    //    IMetricsScope mockScope = mock(CWMetricsScope.class);
    //    MetricsHelper.setMetricsScope(mockScope);
    //    adapterClient.getRecords(GetRecordsRequest.builder().withShardIterator(TEST_STRING));
    //    verify(mockScope, times(0)).addData(eq(MILLIS_BEHIND_LATEST_METRIC), anyDouble(), eq(StandardUnit.Milliseconds), eq(MetricsLevel.SUMMARY));
    //    MetricsHelper.unsetMetricsScope();
    //}
    //
    //@Test
    //public void testNegativeMillisBehindNow() {
    //    StreamRecord streamRecordWithHighApproximateCreationTime =
    //            new StreamRecord().withKeys(KEYS).withSequenceNumber(TEST_STRING).withSizeBytes(0L).withStreamViewType(StreamViewType.KEYS_ONLY).withApproximateCreationDateTime(new Date(System.currentTimeMillis() + 100000));
    //
    //    Record recordToTestNegativeMillisBehindLatest =
    //            new Record().withAwsRegion(TEST_STRING).withDynamodb(streamRecordWithHighApproximateCreationTime).withEventID(TEST_STRING).withEventName(OperationType.INSERT).withEventSource(TEST_STRING)
    //                        .withEventVersion(TEST_STRING);
    //
    //    IMetricsScope mockScope = mock(CWMetricsScope.class);
    //    MetricsHelper.setMetricsScope(mockScope);
    //    when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).thenReturn(GetRecordsResponse.builder().withRecords(recordToTestNegativeMillisBehindLatest));
    //    adapterClient.getRecords(GetRecordsRequest.builder().withShardIterator(TEST_STRING));
    //    verify(mockScope, times(1)).addData(eq(MILLIS_BEHIND_LATEST_METRIC), eq(0.0), eq(StandardUnit.Milliseconds), eq(MetricsLevel.SUMMARY));
    //    MetricsHelper.unsetMetricsScope();
    //}

    @Test(expected = UnsupportedOperationException.class)
    public void testDescreaseStreamRetentionPeriod() {
        adapterClient.decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest.builder().build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIncreaseStreamRetentionPeriod() {
        adapterClient.increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder().build());
    }
}