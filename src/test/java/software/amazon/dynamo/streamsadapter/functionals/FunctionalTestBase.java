/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.functionals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.dynamo.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import software.amazon.dynamo.streamsadapter.StreamsWorkerFactory;
import software.amazon.dynamo.streamsadapter.util.TestRecordProcessorFactory;
import software.amazon.dynamo.streamsadapter.util.TestUtil;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.coordinator.Scheduler;

/**
 * This base class sets up DynamoDB, Kinesis Adapter and DynamoDB streams clients used by a KCL worker operating on DynamoDB
 * Streams. It also creates required DynamoDB tables.
 */
public abstract class FunctionalTestBase {
    private static final Log LOG = LogFactory.getLog(FunctionalTestBase.class);

    protected AmazonDynamoDBLocal dynamoDBLocal;
    protected DynamoDbStreamsClient streamsClient;
    protected AmazonDynamoDBStreamsAdapterClient adapterClient;
    protected DynamoDbClient dynamoDBClient;

    protected DynamoDbAsyncClient dynamoDBAsyncClient;

    protected AwsCredentialsProvider credentials;
    protected String streamId;

    protected Scheduler worker;
    protected TestRecordProcessorFactory recordProcessorFactory;
    protected ExecutorService workerThread;

    private static String accessKeyId = "KCLIntegTest";
    private static String secretAccessKey = "dummy";

    protected static String srcTable = "kcl-integ-test-src";
    protected static String destTable = "kcl-integ-test-dest";
    protected static String leaseTable = "kcl-integ-test-leases";

    protected static int THREAD_SLEEP_5S = 5000;
    protected static int THREAD_SLEEP_2S = 2000;
    protected static String KCL_WORKER_ID = "kcl-integration-test-worker";

    @Before
    public void setup() {
        credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));

        dynamoDBLocal = DynamoDBEmbedded.create();
        dynamoDBClient = dynamoDBLocal.dynamoDbClient();
        dynamoDBAsyncClient = dynamoDBLocal.dynamoDbAsyncClient();
        streamsClient = dynamoDBLocal.dynamoDbStreamsClient();
        dynamoDBLocal.triggerShardRollovers();

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient);

        streamId = TestUtil.createTable(dynamoDBClient, srcTable, true /*With streams enabled*/);
        TestUtil.createTable(dynamoDBClient, destTable, false /* No streams */);

        TestUtil.waitForTableActive(dynamoDBClient, srcTable);
        TestUtil.waitForTableActive(dynamoDBClient, destTable);
    }

    @After
    public void teardown() {
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(srcTable).build());
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(destTable).build());
        dynamoDBClient.deleteTable(DeleteTableRequest.builder().tableName(leaseTable).build());             

        dynamoDBLocal.shutdown();
    }

    protected void startKCLWorker(KinesisClientLibConfiguration workerConfig) {

        recordProcessorFactory = new TestRecordProcessorFactory(dynamoDBClient, destTable);

        LOG.info("Creating worker for stream: " + streamId);
        worker = StreamsWorkerFactory
            .createDynamoDbStreamsWorker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBAsyncClient, Executors.newCachedThreadPool());

        LOG.info("Starting worker...");
        workerThread = Executors.newSingleThreadExecutor();
        workerThread.submit(worker);

        workerThread.shutdown(); //This will wait till the KCL worker exits
    }

    protected void shutDownKCLWorker() throws Exception {
        worker.shutdown();

        if (!workerThread.awaitTermination(THREAD_SLEEP_5S, TimeUnit.MILLISECONDS)) {
            workerThread.shutdownNow();
        }

        LOG.info("Processing complete.");
    }

}