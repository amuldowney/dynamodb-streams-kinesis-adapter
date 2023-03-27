/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import software.amazon.awssdk.services.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.common.ConfigsBuilder;
import com.amazonaws.ClientConfiguration;
//import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
//import com.amazonaws.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
//import com.amazonaws.regions.Regions;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDB
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.leases.StreamsLeaseTaker;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.LeaderDecider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategyType;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.getMetricsFactory;

/**
 * The StreamsWorkerFactory uses the Kinesis Client Library's Worker
 * class to provide convenient constructors for ease-of-use.
 */
public class StreamsWorkerFactory {
    private static final Log LOG = LogFactory.getLog(StreamsWorkerFactory.class);
    /**
     * Factory method.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config, ExecutorService execService) {
        AmazonDynamoDBStreamsAdapterClient streamsClient = new AmazonDynamoDBStreamsAdapterClient(
            config.getKinesisCredentialsProvider(),
            config.getKinesisClientConfiguration());
        AmazonDynamoDB dynamoDBClient = createClient(DynamoDbClientBuilder.standard(),
            config.getDynamoDBCredentialsProvider(),//ConfigsBuilder
            config.getDynamoDBClientConfiguration(),//ConfigsBuilder
            config.getDynamoDBEndpoint(),//ConfigsBuilder
            config.getRegionName());//ConfigsBuilder
        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());//ConfigsBuilder, ConfigsBuilder?? TODO find getBillingMode in v2

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);

        AmazonCloudWatch cloudWatchClient = createClient(AmazonCloudWatchClientBuilder.standard(),
                config.getCloudWatchCredentialsProvider(),
                config.getCloudWatchClientConfiguration(),
                null,
                config.getRegionName());
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),//ConfigsBuilder
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),//RetrievalConfig
                        config.shouldCleanupLeasesUponShardCompletion(),//LeaseManagementConfig
                        config.shouldIgnoreUnexpectedChildShards(),//LeaseManagementConfig
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),//LeaseManagementConfig? TODO find
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());//LeaseManagementConfig? TODO find

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .execService(execService)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())//CoordinatorConfig
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())//ConfigsBuilder, LeaseManagementConfig
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))//LeaseManagementConfig
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());
        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory         Metrics factory used to emit metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory         Metrics factory used to emit metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }


    private static DynamoDBStreamsProxy getDynamoDBStreamsProxy(KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient) {
        return new DynamoDBStreamsProxy.Builder(
            config.getStreamName(),
            config.getKinesisCredentialsProvider(),
            streamsClient)
            .build();
    }

    /*
     * Method to create AWS using provided builders.
     * @param builder Builder used to construct the client object.
     * @param credentialsProvider Provides credentials to access AWS services
     * @param clientConfiguration client Configuration that will be used by the client object.
     * @param endpointUrl The endpoint used for communication
     * @param region The region name for the service.
     */
    static private <R, T extends AwsClientBuilder<T, R>> R createClient(final T builder,
        final AwsCredentialsProvider credentialsProvider,
        final ClientOverrideConfiguration clientConfiguration,
        final String endpointUrl,
        final String region) {
        if (credentialsProvider != null) {
            builder.credentialsProvider(credentialsProvider);
        }
        if (clientConfiguration != null) {
            //TODO check https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#13-sdk-client-configuration
            //for proxy config as well
            builder.overrideConfiguration(clientConfiguration);
        }
        if (!StringUtils.isNullOrEmpty(endpointUrl)) {
            LOG.warn("Received configuration for endpoint as " + endpointUrl + ", and region as "
                + region + ".");
            builder.endpointOverride(URI.create(endpointUrl));
        } else if (!StringUtils.isNullOrEmpty(region)) {
            LOG.warn("Received configuration for region as " + region + ".");
            builder.region(Region.of(region));
        } else {
            LOG.warn("No configuration received for endpoint and region, will default region to us-east-1");
            builder.region(Region.US_EAST_1);
        }
        return builder.build();
    }
}
