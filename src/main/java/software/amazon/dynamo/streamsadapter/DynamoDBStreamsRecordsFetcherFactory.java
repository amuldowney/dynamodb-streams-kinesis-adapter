package software.amazon.dynamo.streamsadapter;

import scala.Int;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.DataFetchingStrategy;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;

/** DynamoDBStreamsRecordsFetcherFactory is an odd helper class that allows us to use the
 * SynchonousPrefetchingRetrievalFactory, which only uses a sparse few of the methods from
 * the RecordsFetcherFactory passed in
 *
 */

public class DynamoDBStreamsRecordsFetcherFactory  implements RecordsFetcherFactory {

  @Override public int maxPendingProcessRecordsInput() {
    return 2000;
  }

  @Override public int maxByteSize() {
    return Integer.MAX_VALUE;
  }


  @Override public int maxRecordsCount() {
    return 1000;
  }

  @Override public void maxByteSize(int maxByteSize) {

  }

  @Override public void maxRecordsCount(int maxRecordsCount) {

  }

  @Override public void dataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy) {

  }

  @Override public DataFetchingStrategy dataFetchingStrategy() {
    return null;
  }

  @Override public void idleMillisBetweenCalls(long idleMillisBetweenCalls) {

  }

  @Override public long idleMillisBetweenCalls() {
    return 0;
  }

  @Override public RecordsPublisher createRecordsFetcher(
      GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId,
      MetricsFactory metricsFactory, int maxRecords) {
    return null;
  }

  @Override public void maxPendingProcessRecordsInput(int maxPendingProcessRecordsInput) {

  }
}