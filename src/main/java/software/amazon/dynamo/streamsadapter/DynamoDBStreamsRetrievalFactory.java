//package software.amazon.dynamo.streamsadapter;
//
//import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
//import software.amazon.kinesis.common.StreamConfig;
//import software.amazon.kinesis.leases.ShardInfo;
//import software.amazon.kinesis.metrics.MetricsFactory;
//import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
//import software.amazon.kinesis.retrieval.RecordsPublisher;
//import software.amazon.kinesis.retrieval.RetrievalFactory;
//
//public class DynamoDBStreamsRetrievalFactory implements RetrievalFactory {
//    private final KinesisAsyncClient kinesisClient;
//
//
//  public DynamoDBStreamsRetrievalFactory(KinesisAsyncClient kinesisClient) {
//    this.kinesisClient = kinesisClient;
//  }
//
//    public GetRecordsRetrievalStrategy createGetRecordsRetrievalStrategy(ShardInfo shardInfo, MetricsFactory metricsFactory) {
//      return null;
//    }
//
//    public RecordsPublisher createGetRecordsCache(ShardInfo shardInfo, StreamConfig streamConfig, MetricsFactory metricsFactory) {
//      if (shardInfo == null) {
//        throw new NullPointerException("shardInfo is marked non-null but is null");
//      } else {
//          return new DynamoDBStreamsDataFetcher(this.kinesisClient, shardInfo);
//        }
//    }
//
//    public RecordsPublisher createGetRecordsCache(ShardInfo shardInfo, MetricsFactory metricsFactory) {
//      throw new UnsupportedOperationException("DynamoDBStreamsRetrievalFactory needs StreamConfig Info");
//    }
//}