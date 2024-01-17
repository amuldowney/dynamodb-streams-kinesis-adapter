/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.dynamo.streamsadapter.model.ShardMapper;

public class TestUtil {
    private static final String STARTING_SEQUENCE_NUMBER = "1";
    private static final String ENDING_SEQUENCE_NUMBER = "2";
    private static final String NULL_SEQUENCE_NUMBER = null;
    /**
     * @return StreamId
     */
    public static String createTable(DynamoDbClient client, String tableName, Boolean withStream) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName("Id").attributeType("N").build());

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(KeySchemaElement.builder().attributeName("Id").keyType(KeyType.HASH).build());

        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(20L).writeCapacityUnits(20L).build();

        CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder().tableName(tableName).attributeDefinitions(attributeDefinitions).keySchema(keySchema)
            .provisionedThroughput(provisionedThroughput);

        if (withStream) {
            createTableRequest.streamSpecification(
                StreamSpecification.builder()
                .streamEnabled(true)
                .streamViewType(StreamViewType.NEW_IMAGE).build());
        }

        try {
            CreateTableResponse result = client.createTable(createTableRequest.build());
            return result.tableDescription().latestStreamArn();
        } catch (ResourceInUseException e) {
            return describeTable(client, tableName).table().latestStreamArn();
        }
    }

    public static void waitForTableActive(DynamoDbClient client, String tableName) throws IllegalStateException {
        int retries = 0;
        boolean created = false;
        while (!created && retries < 100) {
            DescribeTableResponse result = describeTable(client, tableName);
            created = result.table().tableStatus().equals(TableStatus.ACTIVE);
            if (created) {
                return;
            } else {
                retries++;
                try {
                    Thread.sleep(500 + 100 * retries);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        throw new IllegalStateException("Table not active!");
    }

    public static void updateTable(DynamoDbClient client, String tableName, Long readCapacity, Long writeCapacity) {
        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(readCapacity).writeCapacityUnits(writeCapacity).build();

        UpdateTableRequest updateTableRequest = UpdateTableRequest.builder().tableName(tableName).provisionedThroughput(provisionedThroughput).build();

        client.updateTable(updateTableRequest);
    }

    public static DescribeTableResponse describeTable(DynamoDbClient client, String tableName) {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    }

    public static StreamDescription describeStream(DynamoDbStreamsClient client, String streamArn, String lastEvaluatedShardId) {
        DescribeStreamResponse result = client.describeStream(DescribeStreamRequest.builder().streamArn(streamArn).exclusiveStartShardId(lastEvaluatedShardId).build());
        return result.streamDescription();
    }

    public static ScanResponse scanTable(DynamoDbClient client, String tableName) {
        return client.scan(ScanRequest.builder().tableName(tableName).build());
    }

    public static QueryResponse queryTable(DynamoDbClient client, String tableName, String partitionKey) {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":v_id", AttributeValue.builder().n(partitionKey).build());

        QueryRequest queryRequest = QueryRequest.builder().tableName(tableName).keyConditionExpression("Id = :v_id").expressionAttributeValues(expressionAttributeValues).build();

        return client.query(queryRequest);
    }

    public static void putItem(DynamoDbClient client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> item = new HashMap<>();
        item.put("Id", AttributeValue.builder().n(id).build());
        item.put("attribute-1", AttributeValue.builder().s(val).build());

        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        client.putItem(putItemRequest);
    }

    public static void putItem(DynamoDbClient client, String tableName, java.util.Map<String, AttributeValue> items) {
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(items).build();
        client.putItem(putItemRequest);
    }

    public static void updateItem(DynamoDbClient client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", AttributeValue.builder().n(id).build());

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = AttributeValueUpdate.builder().action(AttributeAction.PUT).value(AttributeValue.builder().s(val).build()).build();
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(key).attributeUpdates(attributeUpdates).build();
        client.updateItem(updateItemRequest);
    }

    public static void deleteItem(DynamoDbClient client, String tableName, String id) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", AttributeValue.builder().n(id).build());

        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        client.deleteItem(deleteItemRequest);
    }

    public static String getShardIterator(DynamoDbStreamsClient client, String streamArn, String shardId) {
        GetShardIteratorResponse result =
            client.getShardIterator(GetShardIteratorRequest.builder().streamArn(streamArn).shardId(shardId).shardIteratorType(ShardIteratorType.TRIM_HORIZON).build());
        return result.shardIterator();
    }

    public static GetRecordsResponse getRecords(DynamoDbStreamsClient client, String shardIterator) {
        GetRecordsResponse result = client.getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).build());
        return result;
    }


    public static Shard createDummyDBShard() {
        return _createDummyShard("parentShardId", "shardId", false);
    }

        /**
         *
         * @param parentShardId Parent Shard ID for the created shard
         * @param shardId Shard ID for the created shard
         * @param closed Whether or not the shard is marked as closed (non-null end sequence number).
         * @return
         */
        public static software.amazon.awssdk.services.kinesis.model.Shard createDummyShard(String parentShardId, String shardId, boolean closed) {
            return ShardMapper.convert(_createDummyShard(parentShardId, shardId, closed));
        }
    private static Shard _createDummyShard(String parentShardId, String shardId, boolean closed) {
        software.amazon.awssdk.services.dynamodb.model.Shard.Builder shard =
            software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                .parentShardId(parentShardId)
                .shardId(shardId);
        if (closed) {
            shard = shard.sequenceNumberRange(getSequenceNumberRange());
        } else {
            shard = shard.sequenceNumberRange(getEndNullSequenceNumberRange());
        }
        return shard.build();
    }
    private static SequenceNumberRange getSequenceNumberRange() {
        return SequenceNumberRange.builder()
            .startingSequenceNumber(STARTING_SEQUENCE_NUMBER)
            .endingSequenceNumber(ENDING_SEQUENCE_NUMBER)
            .build();
    }

    private static SequenceNumberRange getEndNullSequenceNumberRange() {
        return SequenceNumberRange.builder()
            .startingSequenceNumber(STARTING_SEQUENCE_NUMBER)
            .endingSequenceNumber(NULL_SEQUENCE_NUMBER)
            .build();
    }

}