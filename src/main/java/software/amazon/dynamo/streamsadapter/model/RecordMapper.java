/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;

/**
 * A single update notification of a DynamoDB Stream, adapted for use
 * with the Amazon Kinesis model.
 * <p>
 * This class is designed to be used in a single thread only.
 */
public class RecordMapper {

    private static Log LOG = LogFactory.getLog(RecordMapper.class);

    public static final Charset defaultCharset = StandardCharsets.UTF_8;

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    public static software.amazon.awssdk.services.kinesis.model.Record convert(software.amazon.awssdk.services.dynamodb.model.Record record, boolean generateRecordDataBytes) {
        return software.amazon.awssdk.services.kinesis.model.Record.builder()
            .sequenceNumber(record.dynamodb().sequenceNumber())
            .data( getData(record, generateRecordDataBytes))
            .partitionKey(null)
            .approximateArrivalTimestamp(record.dynamodb().approximateCreationDateTime())
            .build();
    }

    //todo might need to keep the internal object here so we can cast it appropriately in the stream processor

    /**
     * This method returns JSON serialized {@link Record} object. However, This is not the best to use the object
     * It is recommended to get an object using {@link #getInternalObject()} and cast appropriately.
     *
     * @return JSON serialization of {@link Record} object. JSON contains only non-null
     * fields of {@link com.amazonaws.services.dynamodbv2.model.Record}. It returns null if serialization fails.
     */
    private static SdkBytes getData(software.amazon.awssdk.services.dynamodb.model.Record record, boolean generateDataBytes) {
        ByteBuffer data;
            if (generateDataBytes) {
                try {
                    data = ByteBuffer.wrap(MAPPER.writeValueAsString(record).getBytes(defaultCharset));
                } catch (JsonProcessingException e) {
                    final String errorMessage = "Failed to serialize stream record to JSON";
                    LOG.error(errorMessage, e);
                    throw new RuntimeException(errorMessage, e);
                }
            } else {
                data = ByteBuffer.wrap(new byte[0]);
            }
        return SdkBytes.fromByteBuffer(data);
    }
}