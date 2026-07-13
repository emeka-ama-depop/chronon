package ai.chronon.integrations.aws

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success}

/** KVStore implementation for the enhanced stats dataset.
  *
  * Stats tables differ from streaming tables in two ways:
  *   - Tables must be time-sorted (hash key + sort key) to support tile range queries.
  *   - Keys are plain Avro-encoded bytes, not TileKey-wrapped Thrift — so doQueryLookups
  *     must skip TileKey deserialization and query by raw partition key + sort key range directly.
  */
class DynamoDBStatsKVStoreImpl(rawDynamoDbClient: DynamoDbAsyncClient,
                               conf: Map[String, String] = Map.empty,
                               daxClientProvider: Option[String => DynamoDbAsyncClient])
    extends DynamoDBKVStoreImpl(rawDynamoDbClient, conf, daxClientProvider) {

  def this(rawDynamoDbClient: DynamoDbAsyncClient, conf: Map[String, String]) =
    this(rawDynamoDbClient, conf, None)

  def this(rawDynamoDbClient: DynamoDbAsyncClient) =
    this(rawDynamoDbClient, Map.empty, None)

  override protected val enableTtl: Boolean = false

  override def create(dataset: String): Unit =
    create(dataset, Map(DynamoDBKVStoreConstants.isTimedSorted -> "true"))

  // Schema/metadata rows are stored with an opaque timestamp, so GetItem (which requires both
  // hash + sort key) can't be used. Query by partition key only and take values.last for latest.
  override protected def doGetLookups(getLookups: Seq[GetRequest]): Seq[Future[GetResponse]] = {
    val defaultTimestamp = Instant.now().toEpochMilli
    getLookups.map { req =>
      val tableInfo = resolveBatchTableInfo(req.dataset)
      queryPartitionOnly(tableInfo.physicalTableName, req.keyBytes)
        .transform {
          case Success(response) =>
            val timedValues = extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
            Success(GetResponse(req, Success(timedValues)))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
    }
  }

  override protected def doQueryLookups(queryLookups: Seq[GetRequest]): Seq[Future[GetResponse]] = {
    val defaultTimestamp = Instant.now().toEpochMilli

    queryLookups.map { req =>
      val tableInfo = resolveBatchTableInfo(req.dataset)
      // Stats keys are raw Avro bytes (no TileKey wrapping), query directly by partition key
      // and sort key range — symmetric with multiPut's non-streaming write path.
      queryPartition(tableInfo.physicalTableName, req.keyBytes, req.startTsMillis.get, req.endTsMillis)
        .transform {
          case Success(response) =>
            val timedValues = extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
            Success(GetResponse(req, Success(timedValues)))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
    }
  }
}
