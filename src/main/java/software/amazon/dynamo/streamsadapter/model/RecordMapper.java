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
import software.amazon.awssdk.services.dynamodb.model.Record;

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

    public static software.amazon.awssdk.services.kinesis.model.Record convert(Record record) {
        return software.amazon.awssdk.services.kinesis.model.Record.builder()
            .sequenceNumber(record.dynamodb().sequenceNumber())
            .data(getData(record))
            .partitionKey(null)
            .approximateArrivalTimestamp(record.dynamodb().approximateCreationDateTime())
            .build();
    }

    /**
     * This method returns JSON serialized {@link Record} object.
     *
     * @return JSON serialization of {@link Record} object. JSON contains only non-null
     * fields of {@link Record}. It returns null if serialization fails.
     */
    private static SdkBytes getData(Record record) {
        ByteBuffer data;
                try {
                    String datum = MAPPER.writeValueAsString(record);
                    data = ByteBuffer.wrap(datum.getBytes(defaultCharset));
                } catch (JsonProcessingException e) {
                    final String errorMessage = "Failed to serialize stream record to JSON";
                    LOG.error(errorMessage, e);
                    throw new RuntimeException(errorMessage, e);
                }
        return SdkBytes.fromByteBuffer(data);
    }
}