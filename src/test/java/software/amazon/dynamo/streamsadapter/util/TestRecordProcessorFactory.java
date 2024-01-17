/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.util;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * This implementation of IRecordProcessorFactory creates a variety of
 * record processors for different testing purposes. The type of processor
 * to be created is determined by the constructor.
 */
public class TestRecordProcessorFactory implements ShardRecordProcessorFactory {
    private ReplicatingRecordProcessor createdProcessor = null;

    private String tableName;
    private DynamoDbClient dynamoDB;


    /**
     * Using this constructor creates a replicating processor for an
     * embedded(in-memory) instance of DynamoDB local
     *
     * @param dynamoDB  DynamoDB client for embedded DynamoDB instance
     * @param tableName The name of the table used for replication
     */
    public TestRecordProcessorFactory(DynamoDbClient dynamoDB, String tableName) {
        this.tableName = tableName;
        this.dynamoDB = dynamoDB;
    }

    @Override public ShardRecordProcessor shardRecordProcessor() {
        createdProcessor = new ReplicatingRecordProcessor(dynamoDB, tableName);
        return createdProcessor;
    }

    /**
     * This method returns -1 under the following conditions:
     * 1. createProcessor() has not yet been called
     * 2. initialize() method on the ReplicatingRecordProcessor instance has not yet been called
     * 3. requestedProcessor is COUNTING
     *
     * @return number of records processed by processRecords
     */
    public int getNumRecordsProcessed() {
        if (createdProcessor == null) {
            return -1;
        }
        return createdProcessor.getNumRecordsProcessed();

    }

    public int getNumProcessRecordsCalls() {
        if (createdProcessor == null) {
            return -1;
        }
        return createdProcessor.getNumProcessRecordsCalls();

    }

}