/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class CountingRecordProcessor implements ShardRecordProcessor {

    private static final Log LOG = LogFactory.getLog(CountingRecordProcessor.class);

    private RecordProcessorTracker tracker;

    private String shardId;
    private Integer checkpointCounter;
    private Integer recordCounter;

    CountingRecordProcessor(RecordProcessorTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.shardId = initializationInput.shardId();
        checkpointCounter = 0;
        recordCounter = 0;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (KinesisClientRecord record : processRecordsInput.records()) {
            recordCounter += 1;
            checkpointCounter += 1;
            if (checkpointCounter % 10 == 0) {
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

}