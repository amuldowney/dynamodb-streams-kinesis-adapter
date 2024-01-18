/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.InitializeTask;
import software.amazon.kinesis.lifecycle.ProcessTask;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@PrepareForTest({IKinesisProxy.class, KinesisDataFetcher.class})
@RunWith(PowerMockRunner.class)
public class KinesisClientLibraryRecordDeserializationTests {

    /* Constants for mocking DynamoDB Streams */
    private static final String STREAM_NAME = "stream-1";
    private static final String SHARD_ID = "shard-000000";
    private static final String SEQUENCE_NUMBER_0 = "0000000000000000";
    private static final String SHARD_ITERATOR = "iterator-0000000000";
    private static final Shard SHARD = new Shard().withShardId(SHARD_ID).withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber(SEQUENCE_NUMBER_0));
    private static final StreamDescription STREAM_DESCRIPTION =
            new StreamDescription().withCreationRequestDateTime(new Date()).withKeySchema().withShards(SHARD).withStreamArn(STREAM_NAME).withStreamStatus(StreamStatus.ENABLED);
    private static final StreamRecord
        STREAM_RECORD_0 = StreamRecord.builder().sequenceNumber(SEQUENCE_NUMBER_0).approximateCreationDateTime(
        Instant.now()).build();
    private static final Record RECORD_0 = Record.builder().dynamodb(STREAM_RECORD_0).build();
    private static final List<Record> RECORDS = Arrays.asList(RECORD_0);

    /* Mocking the DynamoDB Streams client, Kinesis Client Library checkpoint interfaces, and Record Processor */
    private static final DynamoDbStreamsClient DYNAMODB_STREAMS = mock(DynamoDbStreamsClient.class);
    private static final Checkpointer CHECKPOINT = mock(Checkpointer.class);
    private static final ShardRecordProcessorCheckpointer CHECKPOINTER = mock(
        ShardRecordProcessorCheckpointer.class);
    private static final ShardRecordProcessor RECORD_PROCESSOR = mock(ShardRecordProcessor.class);

    /* Construct higher level Kinesis Client Library objects from the primitive mocks */
    private static final AmazonDynamoDBStreamsAdapterClient ADAPTER_CLIENT = new AmazonDynamoDBStreamsAdapterClient(DYNAMODB_STREAMS);
    //private static final IKinesisProxy KINESIS_PROXY =
    //        new KinesisProxyFactory(new StaticCredentialsProvider(new BasicAWSCredentials("NotAnAccessKey", "NotASecretKey")), ADAPTER_CLIENT).getProxy(STREAM_NAME);
    private static final ShardInfo SHARD_INFO = new ShardInfo(SHARD_ID, "concurrencyToken", new ArrayList<String>(), null /*checkpoint*/);
    private static final ExtendedSequenceNumber EXTENDED_SEQUENCE_NUMBER = new ExtendedSequenceNumber(SEQUENCE_NUMBER_0);
    //private static final DynamoDBStreamsDataFetcher KINESIS_DATA_FETCHER = new DynamoDBStreamsDataFetcher(KINESIS_PROXY, SHARD_INFO);
    private static final StreamConfig STREAM_CONFIG =
            new StreamConfig(KINESIS_PROXY, 1000/* RecordLimit */, 0l /* IdleTimeMillis */, false /* callProcessRecordsForEmptyList */, false /* validateSequenceNumberBeforeCheckpointing */,
                    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
    private static final int GET_RECORDS_ITEM_LIMIT = 1000;
    private static final ExtendedSequenceNumber NULL_EXTENDED_SEQUENCE_NUMBER = null;

    @Test
    public void testVerifyKCLProvidesRecordAdapter() throws KinesisClientLibException {
        // Setup mocks
        when(CHECKPOINT.getCheckpointObject(SHARD_ID)).thenReturn(new Checkpoint(ExtendedSequenceNumber.TRIM_HORIZON, NULL_EXTENDED_SEQUENCE_NUMBER));
        when(CHECKPOINTER.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.TRIM_HORIZON);
        when(DYNAMODB_STREAMS.describeStream(any(DescribeStreamRequest.class))).thenReturn(new DescribeStreamResult().withStreamDescription(STREAM_DESCRIPTION));
        when(DYNAMODB_STREAMS.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(new GetShardIteratorResult().withShardIterator(SHARD_ITERATOR));
        when(DYNAMODB_STREAMS.getRecords(any(GetRecordsRequest.class))).thenReturn(new GetRecordsResult().withNextShardIterator(SHARD_ITERATOR).withRecords(RECORDS));


        GetRecordsCache cache = new BlockingGetRecordsCache(GET_RECORDS_ITEM_LIMIT, new SynchronousGetRecordsRetrievalStrategy(KINESIS_DATA_FETCHER));
        // Initialize the Record Processor
        InitializeTask initializeTask = new InitializeTask(SHARD_INFO, RECORD_PROCESSOR, CHECKPOINT, CHECKPOINTER, KINESIS_DATA_FETCHER, 0L /* backoffTimeMillis */, STREAM_CONFIG, cache);
        initializeTask.call();
        // Execute process task
        ProcessTask processTask = new ProcessTask(SHARD_INFO, STREAM_CONFIG, RECORD_PROCESSOR, CHECKPOINTER, KINESIS_DATA_FETCHER, 0L /* backoffTimeMillis */, false /*skipShardSyncAtWorkerInitializationIfLeasesExist*/, cache);
        processTask.call();

        // Verify mocks
        verify(CHECKPOINT).getCheckpointObject(SHARD_ID);
        verify(CHECKPOINTER).setLargestPermittedCheckpointValue(EXTENDED_SEQUENCE_NUMBER);
        verify(CHECKPOINTER).setInitialCheckpointValue(ExtendedSequenceNumber.TRIM_HORIZON);
        verify(CHECKPOINTER).getLastCheckpointValue();
        verify(DYNAMODB_STREAMS, atMost(1)).describeStream(any(DescribeStreamRequest.class));
        verify(DYNAMODB_STREAMS).getShardIterator(any(GetShardIteratorRequest.class));
        verify(DYNAMODB_STREAMS).getRecords(any(GetRecordsRequest.class));
        // Capture the input to the ProcessRecords method
        final ArgumentCaptor<ProcessRecordsInput> processRecordsInputCapture = ArgumentCaptor.forClass(ProcessRecordsInput.class);
        verify(RECORD_PROCESSOR).initialize(any(InitializationInput.class));
        verify(RECORD_PROCESSOR).processRecords(processRecordsInputCapture.capture());

        // Verify the Records are delivered to the Record Processor as RecordAdapter objects
        ProcessRecordsInput processRecordsInput = processRecordsInputCapture.getValue();
        assertNotNull(processRecordsInput);
        assertNotNull(processRecordsInput.getRecords());
        assertEquals(RECORDS.size(), processRecordsInput.getRecords().size());
        for (Record record : processRecordsInput.getRecords()) {
            assertTrue("Kinesis Client Library is unwrapping the DynamoDB Streams Record Adapter", record instanceof RecordMapper);
        }
    }
}