/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.dynamo.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import software.amazon.dynamo.streamsadapter.AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior;
import software.amazon.dynamo.streamsadapter.exceptions.UnableToReadMoreRecordsException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmazonServiceExceptionTransformerTests extends AmazonServiceExceptionTransformer {

  private static final String INTERNAL_FAILURE = "InternalFailure";

  private static final String LIMIT_EXCEEDED_EXCEPTION = "LimitExceededException";

  private static final String REQUEST_ID = "request-id-1";

  private static final String RESOURCE_NOT_FOUND = "ResourceNotFound";

  private static final String SEQUENCE_NUMBER = "Sequence-Number-000000000";

  private static final String SERVICE_NAME = "dynamodb";

  private static final String SHARD_ID = "ShardId";

  private static final String SHARD_ITERATOR = "shard iterator 1";

  private static final int STATUS_CODE_400 = 400;

  private static final int STATUS_CODE_500 = 500;

  private static final String STREAM_NAME = "StreamName";

  private static final String TEST_MESSAGE = "Test Message";

  private static final String THROTTLING_EXCEPTION = "ThrottlingException";

  private static final String TRIMMED_DATA_ACCESS_EXCEPTION = "TrimmedDataAccessException";
  private @Mock DynamoDbStreamsClient streams;

  /**
   * Checks that all AmazonServiceException properties are equal between the expected and actual exceptions.
   *
   * @param expected Exception with expected properties
   * @param actual   Exception generated during the test
   */
  private static void assertSameExceptionProperties(AwsServiceException expected,
      AwsServiceException actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(expected.awsErrorDetails().errorCode(), actual.awsErrorDetails().errorCode());
    if (expected.getMessage() == null && actual.getMessage() != null) {
      assertEquals(EMPTY_STRING, actual.getMessage());
    } else {
      assertEquals(expected.getMessage(), actual.getMessage());
    }
    //todo error type?
    assertEquals(expected.requestId(), actual.requestId());
    assertEquals(expected.awsErrorDetails().serviceName(), actual.awsErrorDetails().serviceName());
    assertEquals(expected.statusCode(), actual.statusCode());
  }

  /**
   * Helper function to set AmazonServiceException properties.
   *
   * @param ase         The Exception to modify
   * @param errorCode   Error code property
   * @param requestId   RequestId property
   * @param serviceName ServiceName property
   * @param statusCode  StatusCode property
   */
  private static AwsServiceException setFields(AwsServiceException.Builder ase, String errorCode,
      String requestId, String serviceName, int statusCode) {
    if (errorCode != null) {
      ase = ase.awsErrorDetails(ase.awsErrorDetails().toBuilder().errorCode(errorCode).build());
    }
    if (requestId != null) {
      ase = ase.requestId(requestId);
    }
    if (serviceName != null) {
      ase = ase.awsErrorDetails(ase.awsErrorDetails().toBuilder().serviceName(serviceName).build());
    }
    return ase.statusCode(statusCode).build();
  }

  @Before
  public void setUpTest() {
    MockitoAnnotations.initMocks(this);
  }

  private void doDescribeStreamTest(AwsServiceException ase, Class<?> expectedResult)
      throws Exception {
    when(streams.describeStream(Matchers.any(DescribeStreamRequest.class))).thenThrow(ase);
    AmazonDynamoDBStreamsAdapterClient adapterClient =
        new AmazonDynamoDBStreamsAdapterClient(streams);
    try {
      adapterClient.describeStream(
          software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
              .streamName(STREAM_NAME)
              .build());
      fail("Expected " + expectedResult.getCanonicalName());
    } catch (AwsServiceException e) {
      assertEquals(expectedResult, e.getClass());
      assertSameExceptionProperties(ase, e);
    }
    verify(streams, Mockito.times(1)).describeStream(Matchers.any(DescribeStreamRequest.class));
  }

  private void doGetRecordsTest(AwsServiceException ase, Class<?> expectedResult,
      SkipRecordsBehavior skipRecordsBehavior) throws Exception {
    when(streams.getRecords(Matchers.any(GetRecordsRequest.class))).thenThrow(ase);
    AmazonDynamoDBStreamsAdapterClient adapterClient =
        new AmazonDynamoDBStreamsAdapterClient(streams);
    adapterClient.setSkipRecordsBehavior(skipRecordsBehavior);
    try {
      adapterClient.getRecords(
          software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder()
              .shardIterator(SHARD_ITERATOR)
              .build());
      fail("Expected " + expectedResult.getCanonicalName());
    } catch (RuntimeException e) {
      assertEquals(expectedResult, e.getClass());
      if (e instanceof AwsServiceException) {
        assertSameExceptionProperties(ase, (AwsServiceException) e);
      }
    }
    verify(streams, Mockito.times(1)).getRecords(Matchers.any(GetRecordsRequest.class));
  }

  private void doGetShardIteratorTest(AwsServiceException ase, Class<?> expectedResult,
      SkipRecordsBehavior skipRecordsBehavior, int numCalls) throws Exception {
    when(streams.getShardIterator(Matchers.any(
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.class))).thenThrow(
        ase);
    AmazonDynamoDBStreamsAdapterClient adapterClient =
        new AmazonDynamoDBStreamsAdapterClient(streams);
    adapterClient.setSkipRecordsBehavior(skipRecordsBehavior);
    try {
      adapterClient.getShardIterator(
          GetShardIteratorRequest.builder()
              .streamName(STREAM_NAME)
              .shardId(SHARD_ID)
              .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
              .startingSequenceNumber(SEQUENCE_NUMBER)
              .build());
      fail("Expected " + expectedResult.getCanonicalName());
    } catch (RuntimeException e) {
      assertEquals(expectedResult, e.getClass());
      if (e instanceof AwsServiceException) {
        assertSameExceptionProperties(ase, (AwsServiceException) e);
      }
    }
    verify(streams, Mockito.times(numCalls)).getShardIterator(Matchers.any(
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.class));
  }

  private void doListStreamsTest(AwsServiceException ase, Class<?> expectedResult)
      throws Exception {
    when(streams.listStreams(Matchers.any(ListStreamsRequest.class))).thenThrow(ase);
    AmazonDynamoDBStreamsAdapterClient adapterClient =
        new AmazonDynamoDBStreamsAdapterClient(streams);
    try {
      adapterClient.listStreams();
      fail("Expected " + expectedResult.getCanonicalName());
    } catch (AwsServiceException e) {
      assertEquals(expectedResult, e.getClass());
      assertSameExceptionProperties(ase, e);
    }
    verify(streams, Mockito.times(1)).listStreams(Matchers.any(ListStreamsRequest.class));
  }

  @Test()
  public void testDescribeStreamInternalServerErrorException() throws Exception {
    AwsServiceException.Builder iseB = InternalServerErrorException.builder().message(TEST_MESSAGE);
    AwsServiceException ise =
        setFields(iseB, INTERNAL_FAILURE, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
    doDescribeStreamTest(ise, com.amazonaws.AmazonServiceException.class);
  }

  @Test
  public void testDescribeStreamIrrelevantException() throws Exception {
    // Not thrown by DynamoDB Streams
    AwsServiceException.Builder exceptionB = ProvisionedThroughputExceededException.builder();
    AwsServiceException exception = setFields(exceptionB, null, null, null, STATUS_CODE_400);
    doDescribeStreamTest(exception,
        com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class);
  }

  @Test()
  public void testDescribeStreamResourceNotFoundException() throws Exception {
    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
    AwsServiceException rnfe =
        setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doDescribeStreamTest(rnfe, ResourceNotFoundException.class);
  }

  @Test()
  public void testDescribeStreamThrottlingException() throws Exception {
    AwsServiceException.Builder teB = AwsServiceException.builder().message(TEST_MESSAGE);
    AwsServiceException te =
        setFields(teB, THROTTLING_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doDescribeStreamTest(te, LimitExceededException.class);
  }

  @Test
  public void testExceptionWithNullFields() throws Exception {
    AwsServiceException.Builder iseB = InternalServerErrorException.builder();
    AwsServiceException ise = setFields(iseB, null, null, null, STATUS_CODE_500);
    doListStreamsTest(ise, AwsServiceException.class);
  }

  @Test()
  public void testGetRecordsExpiredIteratorException() throws Exception {
    AwsServiceException.Builder eieB = ExpiredIteratorException.builder().message(TEST_MESSAGE);
    AwsServiceException eie =
        setFields(eieB, INTERNAL_FAILURE, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetRecordsTest(eie, ExpiredIteratorException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  @Test()
  public void testGetRecordsInternalServerErrorException() throws Exception {
    AwsServiceException.Builder iseB = InternalServerErrorException.builder().message(TEST_MESSAGE);
    AwsServiceException ise =
        setFields(iseB, INTERNAL_FAILURE, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
    doGetRecordsTest(ise, AwsServiceException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  @Test
  public void testGetRecordsIrrelevantException() throws Exception {
    // Not thrown by DynamoDB Streams
    AwsServiceException.Builder exceptionB = ProvisionedThroughputExceededException.builder();
    AwsServiceException exception = setFields(exceptionB, null, null, null, STATUS_CODE_400);
    doGetRecordsTest(exception, ProvisionedThroughputExceededException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  @Test()
  public void testGetRecordsLimitExceededException() throws Exception {
    LimitExceededException.Builder teB = LimitExceededException.builder().message(TEST_MESSAGE);
    AwsServiceException te =
        setFields(teB, LIMIT_EXCEEDED_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetRecordsTest(te, ProvisionedThroughputExceededException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  @Test()
  public void testGetRecordsResourceNotFoundExceptionKCLRetry() throws Exception {
    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
    AwsServiceException rnfe =
        setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetRecordsTest(rnfe, ResourceNotFoundException.class, SkipRecordsBehavior.KCL_RETRY);
  }

  @Test()
  public void testGetRecordsResourceNotFoundExceptionSkipRecords() throws Exception {
    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
    AwsServiceException rnfe =
        setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetRecordsTest(rnfe, ResourceNotFoundException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  @Test()
  public void testGetRecordsThrottlingException() throws Exception {
    AwsServiceException.Builder teB = AwsServiceException.builder().message(TEST_MESSAGE);
    AwsServiceException te =
        setFields(teB, THROTTLING_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetRecordsTest(te, ProvisionedThroughputExceededException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  }

  //@Test()
  //public void testGetRecordsTrimmedDataAccessExceptionKCLRetry() throws Exception {
  //    TrimmedDataAccessException.Builder tdae = new TrimmedDataAccessException(TEST_MESSAGE);
  //    setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doGetRecordsTest(tdae, UnableToReadMoreRecordsException.class, SkipRecordsBehavior.KCL_RETRY);
  //}
  //
  //@Test()
  //public void testGetRecordsTrimmedDataAccessExceptionSkipRecords() throws Exception {
  //    TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
  //    setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doGetRecordsTest(tdae, com.amazonaws.services.kinesis.model.ExpiredIteratorException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
  //}
  //
  //@Test
  //public void testGetShardIteratorInternalServerErrorException() throws Exception {
  //    InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
  //    setFields(ise, INTERNAL_FAILURE, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
  //    doGetShardIteratorTest(ise, com.amazonaws.AmazonServiceException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
  //}
  //
  //@Test
  //public void testGetShardIteratorIrrelevantException() throws Exception {
  //    // Not thrown by DynamoDB Streams
  //    ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
  //    setFields(exception, null, null, null, STATUS_CODE_400);
  //    doGetShardIteratorTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON,
  //        1);
  //}

  @Test()
  public void testGetShardIteratorResourceNotFoundExceptionKCLRetry() throws Exception {
    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
    AwsServiceException rnfe =
        setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetShardIteratorTest(rnfe, UnableToReadMoreRecordsException.class,
        SkipRecordsBehavior.KCL_RETRY, 1);
  }

  @Test()
  public void testGetShardIteratorResourceNotFoundExceptionSkipRecords() throws Exception {
    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
    AwsServiceException rnfe =
        setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
    doGetShardIteratorTest(rnfe, ResourceNotFoundException.class,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
  }

  //@Test()
  //public void testGetShardIteratorThrottlingException() throws Exception {
  //    AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
  //    setFields(te, THROTTLING_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doGetShardIteratorTest(te, com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
  //}
  //
  //@Test()
  //public void testGetShardIteratorTrimmedDataAccessExceptionKCLRetry() throws Exception {
  //    TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
  //    setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doGetShardIteratorTest(tdae, UnableToReadMoreRecordsException.class, SkipRecordsBehavior.KCL_RETRY, 1);
  //}
  //
  //@Test()
  //public void testGetShardIteratorTrimmedDataAccessExceptionSkipRecords() throws Exception {
  //    TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
  //    setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doGetShardIteratorTest(tdae, ResourceNotFoundException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 2);
  //}
  //
  //@Test
  //public void testListStreamsInternalServerErrorException() throws Exception {
  //    InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
  //    setFields(ise, INTERNAL_FAILURE, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
  //    doListStreamsTest(ise, com.amazonaws.AmazonServiceException.class);
  //}
  //
  //@Test
  //public void testListStreamsIrrelevantException() throws Exception {
  //    // Not thrown by DynamoDB Streams
  //    ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
  //    setFields(exception, null, null, null, STATUS_CODE_400);
  //    doListStreamsTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class);
  //}
  //
  //@Test
  //public void testListStreamsResourceNotFoundException() throws Exception {
  //    ResourceNotFoundException.Builder rnfeB = ResourceNotFoundException.builder();
  //    AwsServiceException rnfe =  setFields(rnfeB, RESOURCE_NOT_FOUND, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doListStreamsTest(rnfe, com.amazonaws.AmazonServiceException.class);
  //}
  //
  //@Test
  //public void testListStreamsThrottlingException() throws Exception {
  //    AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
  //    setFields(te, THROTTLING_EXCEPTION, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
  //    doListStreamsTest(te, com.amazonaws.services.kinesis.model.LimitExceededException.class);
  //}

  @Test
  public void testNullException() {
    // If exception is null, we should get back null
    assertNull(transformDynamoDBStreamsToKinesisListStreams(null));
    assertNull(transformDynamoDBStreamsToKinesisDescribeStream(null));
    assertNull(transformDynamoDBStreamsToKinesisGetRecords(null, SkipRecordsBehavior.KCL_RETRY));
    assertNull(transformDynamoDBStreamsToKinesisGetRecords(null,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON));
    assertNull(transformDynamoDBStreamsToKinesisGetRecords(null, null));
    assertNull(
        transformDynamoDBStreamsToKinesisGetShardIterator(null, SkipRecordsBehavior.KCL_RETRY));
    assertNull(transformDynamoDBStreamsToKinesisGetShardIterator(null,
        SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON));
    assertNull(transformDynamoDBStreamsToKinesisGetShardIterator(null, null));
  }
}