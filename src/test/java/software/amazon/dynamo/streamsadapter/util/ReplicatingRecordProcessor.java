/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;

import java.nio.charset.StandardCharsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.utils.BinaryUtils;
import software.amazon.dynamo.streamsadapter.model.RecordMapper;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class ReplicatingRecordProcessor implements ShardRecordProcessor {

    private static final Log LOG = LogFactory.getLog(ReplicatingRecordProcessor.class);
    private static final KDSRecordObjectMapper MAPPER = new KDSRecordObjectMapper();

    private DynamoDbClient dynamoDBClient;
    private String tableName;
    private Integer checkpointCounter = -1;
    private Integer processRecordsCallCounter;

    public static final int CHECKPOINT_BATCH_SIZE = 10;

    ReplicatingRecordProcessor(DynamoDbClient dynamoDBClient, String tableName) {
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        checkpointCounter = 0;
        processRecordsCallCounter = 0;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        processRecordsCallCounter++;
        for (KinesisClientRecord record : processRecordsInput.records()) {
            String data = new String(BinaryUtils.copyBytesFrom(record.data()), StandardCharsets.UTF_8);
            LOG.info("Got record: " + data);

            Record ddbRecord;
            try {
                ddbRecord = MAPPER.readValue(BinaryUtils.copyBytesFrom(record.data()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            switch (ddbRecord.eventName()) {
                case INSERT:
                case MODIFY:
                    TestUtil.putItem(dynamoDBClient, tableName, ddbRecord.dynamodb().newImage());
                    break;
                case REMOVE:
                    TestUtil.deleteItem(dynamoDBClient, tableName, ddbRecord.dynamodb().keys().get("Id").n());
                    break;
            }
            checkpointCounter += 1;
            if (checkpointCounter % CHECKPOINT_BATCH_SIZE == 0) {
                try {
                    processRecordsInput.checkpointer().checkpoint(record.sequenceNumber());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownInput) {
            try {
                shutdownInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {

    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (
                 InvalidStateException |
                 ShutdownException e) {
            //
            // Swallow the exception
            //
            e.printStackTrace();
        }
    }

    int getNumRecordsProcessed() {
        return checkpointCounter;
    }

    int getNumProcessRecordsCalls() {
        return processRecordsCallCounter;
    }

}