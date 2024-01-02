/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.util;

import java.nio.charset.Charset;

import java.nio.charset.StandardCharsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
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
            String data = new String(record.data().array(), StandardCharsets.UTF_8);
            LOG.info("Got record: " + data);

            checkpointCounter += 1;//should go under the commented out work
            if (checkpointCounter % CHECKPOINT_BATCH_SIZE == 0) {//should go under the commented out work
                try {
                    processRecordsInput.checkpointer().checkpoint(record.sequenceNumber());
                } catch (Exception e) {
                    e.printStackTrace();
                }//should go under the commented out work
            }
            throw new RuntimeException("We did it! Fix me now");
            //if (record instanceof RecordMapper) {
            //    com.amazonaws.services.dynamodbv2.model.Record usRecord = ((RecordMapper) record).getInternalObject();
            //    switch (usRecord.getEventName()) {
            //        case "INSERT":
            //        case "MODIFY":
            //            TestUtil.putItem(dynamoDBClient, tableName, usRecord.getDynamodb().getNewImage());
            //            break;
            //        case "REMOVE":
            //            TestUtil.deleteItem(dynamoDBClient, tableName, usRecord.getDynamodb().getKeys().get("Id").getN());
            //            break;
            //    }
            //}
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