/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.utils.BinaryUtils;

@PrepareForTest({ObjectMapper.class, RecordMapper.class})
@RunWith(PowerMockRunner.class)
public class RecordMapperTest {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final String TEST_STRING = "TestString";

    private static final Instant TEST_DATE = new Date(1156377600 /* EC2 Announced */).toInstant();

    private Record dynamoRecord;

    private software.amazon.awssdk.services.kinesis.model.Record kinesisRecord;

    @Before
    public void setUpTest() {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", AttributeValue.builder().s("hashKeyValue").build());
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", AttributeValue.builder().s("someValue").build());

        dynamoRecord = Record.builder()
        .awsRegion("us-east-1")
        .eventID(UUID.randomUUID().toString())
        .eventSource("aws:dynamodb")
        .eventVersion("1.1")
        .eventName(OperationType.MODIFY)
        .dynamodb(StreamRecord.builder()
            .approximateCreationDateTime(TEST_DATE)
            .keys(key)
            .oldImage(new HashMap<>(key))
            .newImage(newImage)
            .sizeBytes(Long.MAX_VALUE)
            .sequenceNumber(TEST_STRING)
            .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
            .build()).build();

        kinesisRecord = RecordMapper.convert(dynamoRecord);
    }

    @Test
    public void testGetSequenceNumber() {
        assertEquals(TEST_STRING, kinesisRecord.sequenceNumber());
    }

    @Test
    public void testGetPartitionKey() {
        assertEquals(kinesisRecord.partitionKey(), null);
    }

    @Test
    public void testGetDataDeserialized() throws  IOException {
        Whitebox.setInternalState(RecordMapper.class, ObjectMapper.class, MAPPER);

        java.nio.ByteBuffer dynamoRecordAsData = kinesisRecord.data().asByteBuffer();
        Record actual = MAPPER.readValue(BinaryUtils.copyBytesFrom(dynamoRecordAsData), Record.serializableBuilderClass()).build();
        assertEquals(dynamoRecord, actual);
    }
}