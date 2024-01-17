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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

@PrepareForTest({ObjectMapper.class, RecordMapper.class})
@RunWith(PowerMockRunner.class)
public class RecordMapperTest {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final ObjectMapper MOCK_MAPPER = mock(RecordObjectMapper.class);

    private static final String TEST_STRING = "TestString";

    private static final Instant TEST_DATE = new Date(1156377600 /* EC2 Announced */).toInstant();

    private static final String TEST_RECORD_v1_0 =
        new StringBuilder().append("{").append("\"awsRegion\":\"us-east-1\",").append("\"dynamodb\":").append("{").append("\"Keys\":").append("{")
            .append("\"hashKey\":{\"S\":\"hashKeyValue\"}").append("},").append("\"StreamViewType\":\"NEW_AND_OLD_IMAGES\",")
            .append("\"SequenceNumber\":\"100000000003498069978\",").append("\"SizeBytes\":6").append("},").append("\"eventID\":\"33fe21d365c03362c5e66d8dec2b63d5\",")
            .append("\"eventVersion\":\"1.0\",").append("\"eventName\":\"INSERT\",").append("\"eventSource\":\"aws:dynamodb\"").append("}").toString();

    private Record testRecord;

    private software.amazon.awssdk.services.kinesis.model.Record adapter;

    @Before
    public void setUpTest() {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", AttributeValue.builder().s("hashKeyValue").build());
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", AttributeValue.builder().s("someValue").build());
       testRecord = Record.builder()
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

        adapter = RecordMapper.convert(testRecord);
    }

    @Test
    public void testDoesNotGenerateBytesWhenGenerateDataBytesIsFalse() {
        assertEquals(0, adapter.data().asByteArray().length);
    }

    @Test
    public void testGetSequenceNumber() {
        String actual = adapter.sequenceNumber();
        assertEquals(TEST_STRING, actual);
    }
    //
    ///**
    // * We need a custom serializer/deserializer to be able to process Record object because of the conflicts that arise
    // * with the standard jackson mapper for fields like eventName etc.
    // */
    //@Test
    //public void testGetDataDeserialized() throws JsonParseException, JsonMappingException, IOException {
    //    Whitebox.setInternalState(RecordMapper.class, ObjectMapper.class, MAPPER);
    //
    //    java.nio.ByteBuffer data = adapter.data().asByteBuffer();
    //    Record actual = MAPPER.readValue(data.array(), Record.class);
    //    assertEquals(adapter, actual);
    //}


    @Test
    public void testRecord_v1_0() throws IOException {
        Record deserialized = MAPPER.readValue(TEST_RECORD_v1_0, Record.class);
        software.amazon.awssdk.services.kinesis.model.Record adapter = RecordMapper.convert(deserialized);
        assertNull(adapter.approximateArrivalTimestamp());
    }

}