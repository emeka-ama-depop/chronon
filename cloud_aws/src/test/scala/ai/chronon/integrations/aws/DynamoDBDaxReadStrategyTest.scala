package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{KvDaxEndpointArg, KvEnableDaxArg}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TilingUtils
import ai.chronon.integrations.aws.DynamoDBKVStoreConstants._
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

class DynamoDBDaxReadStrategyTest extends AnyFlatSpec with Matchers {

  private val daxEndpoint = "dax://test-cluster.example.com"
  private val dataset = "TEAM_FEATURE_STREAMING"
  private val entityKey = "entity-1".getBytes(StandardCharsets.UTF_8)
  private val tileSizeMillis = 1.hour.toMillis
  private val dayStart = 1728000000000L

  "DynamoDBKVStoreImpl" should "use DAX BatchGetItem for timed streaming reads" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = echoBatchGetItems)
    val store = new DynamoDBKVStoreImpl(
      rawClient,
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> daxEndpoint),
      Some(_ => daxClient)
    )
    val request = tiledRequest(dayStart + 30.minutes.toMillis, dayStart + 3.hours.toMillis + 30.minutes.toMillis)

    val response = Await.result(store.multiGet(Seq(request)), 5.seconds).head

    rawClient.queryRequests shouldBe empty
    daxClient.queryRequests shouldBe empty
    daxClient.batchGetRequests should have size 1

    val expectedTimestamps = Seq(dayStart + 1.hour.toMillis, dayStart + 2.hours.toMillis, dayStart + 3.hours.toMillis)
    response.values.get.map(_.millis) shouldBe expectedTimestamps

    val requestedKeys = daxClient.batchGetRequests.head.requestItems().get(dataset).keys().toScala
    requestedKeys.map(_.get(sortKeyColumn).n().toLong) should contain theSameElementsAs expectedTimestamps
    requestedKeys.foreach { key =>
      val timestamp = key.get(sortKeyColumn).n().toLong
      key.get(partitionKeyColumn).b().asByteArray() shouldBe
        buildKeyWithTileSize(entityKey, timestamp, tileSizeMillis)
    }
  }

  it should "keep timed streaming reads on DynamoDB Query when DAX is disabled" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = echoBatchGetItems)
    val store = new DynamoDBKVStoreImpl(
      rawClient,
      Map(KvEnableDaxArg -> "false", KvDaxEndpointArg -> daxEndpoint),
      Some(_ => daxClient)
    )

    Await.result(store.multiGet(Seq(tiledRequest(dayStart, dayStart + 1.hour.toMillis))), 5.seconds)

    rawClient.queryRequests should have size 1
    rawClient.batchGetRequests shouldBe empty
    daxClient.queryRequests shouldBe empty
    daxClient.batchGetRequests shouldBe empty
  }

  it should "keep timed streaming reads on DynamoDB Query when no DAX endpoint can be resolved" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = echoBatchGetItems)
    val store = new DynamoDBKVStoreImpl(rawClient, Map(KvEnableDaxArg -> "true"), Some(_ => daxClient))

    Await.result(store.multiGet(Seq(tiledRequest(dayStart, dayStart + 1.hour.toMillis))), 5.seconds)

    rawClient.queryRequests should have size 1
    daxClient.queryRequests shouldBe empty
    daxClient.batchGetRequests shouldBe empty
  }

  it should "reuse the sibling batch registry endpoint for per-GroupBy streaming reads" in {
    val registryEndpoint = "dax://group-specific.example.com"
    val rawClient = new RecordingDynamoDbAsyncClient(getItemHandler = request => {
      if (request.tableName() == batchTableRegistry)
        completedGetItem(s"$registryEndpoint@TEAM_FEATURE_BATCH_PHYSICAL")
      else GetItemResponse.builder().build()
    })
    val globalDaxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = echoBatchGetItems)
    val registryDaxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = echoBatchGetItems)
    val store = new DynamoDBKVStoreImpl(
      rawClient,
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> daxEndpoint),
      Some(endpoint => if (endpoint == registryEndpoint) registryDaxClient else globalDaxClient)
    )

    Await.result(store.multiGet(Seq(tiledRequest(dayStart, dayStart + 1.hour.toMillis))), 5.seconds)

    registryDaxClient.batchGetRequests should have size 1
    globalDaxClient.batchGetRequests shouldBe empty
  }

  it should "reuse the sibling batch registry endpoint for streaming writes" in {
    val registryEndpoint = "dax://group-specific.example.com"
    val rawClient = new RecordingDynamoDbAsyncClient(getItemHandler = request => {
      if (request.tableName() == batchTableRegistry)
        completedGetItem(s"$registryEndpoint@TEAM_FEATURE_BATCH_PHYSICAL")
      else GetItemResponse.builder().build()
    })
    val daxClient = new RecordingDynamoDbAsyncClient()
    val store = new DynamoDBKVStoreImpl(
      rawClient,
      Map(KvEnableDaxArg -> "true"),
      Some(endpoint => {
        endpoint shouldBe registryEndpoint
        daxClient
      })
    )
    val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), Some(dayStart))
    val request = PutRequest(TilingUtils.serializeTileKey(tileKey), "value".getBytes(StandardCharsets.UTF_8), dataset)

    Await.result(store.multiPut(Seq(request)), 5.seconds) shouldBe Seq(true)

    rawClient.getItemRequests.map(_.tableName()) shouldBe Seq(batchTableRegistry)
    rawClient.putItemRequests shouldBe empty
    daxClient.putItemRequests.map(_.tableName()) shouldBe Seq(dataset)
  }

  it should "re-resolve the DAX endpoint for each streaming write" in {
    val firstEndpoint = "dax://first.example.com"
    val secondEndpoint = "dax://second.example.com"
    val firstDaxClient = new RecordingDynamoDbAsyncClient()
    val secondDaxClient = new RecordingDynamoDbAsyncClient()
    var resolutionCount = 0
    val store = new DynamoDBKVStoreImpl(
      new RecordingDynamoDbAsyncClient(),
      Map(KvEnableDaxArg -> "true"),
      Some(endpoint => if (endpoint == firstEndpoint) firstDaxClient else secondDaxClient)
    ) {
      override private[aws] def resolveBatchTableInfo(requestedDataset: String): BatchTableInfo = {
        requestedDataset shouldBe dataset
        resolutionCount += 1
        val endpoint = if (resolutionCount == 1) firstEndpoint else secondEndpoint
        BatchTableInfo(requestedDataset, Some(endpoint))
      }
    }
    val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), Some(dayStart))
    val request = PutRequest(TilingUtils.serializeTileKey(tileKey), "value".getBytes(StandardCharsets.UTF_8), dataset)

    Await.result(store.multiPut(Seq(request)), 5.seconds) shouldBe Seq(true)
    Await.result(store.multiPut(Seq(request)), 5.seconds) shouldBe Seq(true)

    resolutionCount shouldBe 2
    firstDaxClient.putItemRequests should have size 1
    secondDaxClient.putItemRequests should have size 1
  }

  it should "chunk BatchGetItem requests and retry unprocessed keys" in {
    var returnUnprocessedKey = true
    val daxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = request => {
      val (tableName, keysAndAttributes) = request.requestItems().toScala.head
      val keys = keysAndAttributes.keys().toScala
      if (returnUnprocessedKey && keys.size == BatchGetMaxKeys) {
        returnUnprocessedKey = false
        completedBatchGet(tableName, keys.tail, keys.take(1))
      } else {
        completedBatchGet(tableName, keys, Seq.empty)
      }
    })
    val store = new DynamoDBKVStoreImpl(
      new RecordingDynamoDbAsyncClient(),
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> daxEndpoint),
      Some(_ => daxClient)
    )

    val response = Await
      .result(store.multiGet(Seq(tiledRequest(dayStart, dayStart + 100.hours.toMillis))), 5.seconds)
      .head

    daxClient.batchGetRequests should have size 3
    daxClient.batchGetRequests.map(_.requestItems().get(dataset).keys().size()) should contain allOf (100, 1)
    response.values.get.map(_.millis) shouldBe (0L to 100L).map(offset => dayStart + offset.hours.toMillis)
  }

  it should "bound concurrent BatchGetItem requests across a multiGet" in {
    val pendingResponses = new ConcurrentLinkedQueue[(BatchGetItemRequest, CompletableFuture[BatchGetItemResponse])]()
    val daxClient = new RecordingDynamoDbAsyncClient(
      batchGetFutureHandler = Some(request => {
        val response = new CompletableFuture[BatchGetItemResponse]()
        pendingResponses.add(request -> response)
        response
      }))
    val store = new DynamoDBKVStoreImpl(
      new RecordingDynamoDbAsyncClient(),
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> daxEndpoint),
      Some(_ => daxClient)
    )

    val result = store.multiGet(Seq.fill(2)(tiledRequest(dayStart, dayStart + 499.hours.toMillis)))

    pendingResponses.size() shouldBe BatchGetMaxConcurrentRequests
    (1 to 10).foreach { _ =>
      awaitCondition(!pendingResponses.isEmpty)
      pendingResponses.size() should be <= BatchGetMaxConcurrentRequests
      val (request, response) = pendingResponses.poll()
      response.complete(echoBatchGetItems(request))
    }

    Await.result(result, 5.seconds).flatMap(_.values.get) should have size 1000
    daxClient.batchGetRequests should have size 10
  }

  it should "ignore a registry DAX endpoint when the local DAX flag is disabled" in {
    val physicalTable = "TEAM_FEATURE_BATCH_2026_07_13_123"
    val rawClient = new RecordingDynamoDbAsyncClient(getItemHandler = request => {
      val value = if (request.tableName() == batchTableRegistry) s"$daxEndpoint@$physicalTable" else "batch-value"
      completedGetItem(value)
    })
    val daxClient = new RecordingDynamoDbAsyncClient()
    val store = new DynamoDBKVStoreImpl(rawClient, Map(KvEnableDaxArg -> "false"), Some(_ => daxClient))
    val request = GetRequest(entityKey, "TEAM_FEATURE_BATCH")

    val response = Await.result(store.multiGet(Seq(request)), 5.seconds).head

    response.values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8)) shouldBe Seq("batch-value")
    rawClient.getItemRequests.map(_.tableName()) shouldBe Seq(batchTableRegistry, physicalTable)
    daxClient.getItemRequests shouldBe empty
  }

  it should "keep enhanced stats range reads on raw DynamoDB Query" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient()
    val store = new DynamoDBStatsKVStoreImpl(
      rawClient,
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> daxEndpoint),
      Some(_ => daxClient)
    )
    val request = GetRequest(entityKey, "ENHANCED_STATS", Some(dayStart), Some(dayStart + 1.hour.toMillis))

    Await.result(store.multiGet(Seq(request)), 5.seconds)

    rawClient.queryRequests should have size 1
    daxClient.queryRequests shouldBe empty
    daxClient.batchGetRequests shouldBe empty
  }

  it should "reject a DAX endpoint without the required scheme" in {
    an[IllegalArgumentException] should be thrownBy new DynamoDBKVStoreImpl(
      new RecordingDynamoDbAsyncClient(),
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> "test-cluster.example.com")
    )
  }

  "buildTimeSeriesGetKeys" should "match inclusive Query bounds across day partitions" in {
    val keys = buildTimeSeriesGetKeys(
      entityKey,
      dayStart + 30.minutes.toMillis,
      dayStart + 1.day.toMillis + 90.minutes.toMillis,
      tileSizeMillis
    )

    val timestamps = keys.map(_(sortKeyColumn).n().toLong)
    timestamps.head shouldBe dayStart + 1.hour.toMillis
    timestamps.last shouldBe dayStart + 1.day.toMillis + 1.hour.toMillis
    keys.head(partitionKeyColumn).b().asByteArray() shouldBe
      buildKeyWithTileSize(entityKey, timestamps.head, tileSizeMillis)
    keys.last(partitionKeyColumn).b().asByteArray() shouldBe
      buildKeyWithTileSize(entityKey, timestamps.last, tileSizeMillis)
  }

  "PrefixedDynamoDbAsyncClient" should "keep Query on raw DynamoDB when a DAX delegate is available" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient()
    val client = new PrefixedDynamoDbAsyncClient(rawClient, "prefix_", _ => Some(daxClient))

    client.query(QueryRequest.builder().tableName(dataset).build()).join()

    rawClient.queryRequests.map(_.tableName()) shouldBe Seq(s"prefix_$dataset")
    daxClient.queryRequests shouldBe empty
  }

  it should "route PutItem through DAX with the prefixed table name" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val daxClient = new RecordingDynamoDbAsyncClient()
    val client = new PrefixedDynamoDbAsyncClient(rawClient, "prefix_", _ => Some(daxClient))

    client.putItem(PutItemRequest.builder().tableName(dataset).build()).join()

    rawClient.putItemRequests shouldBe empty
    daxClient.putItemRequests.map(_.tableName()) shouldBe Seq(s"prefix_$dataset")
  }

  it should "route BatchGetItem through DAX and normalize prefixed response table names" in {
    val rawClient = new RecordingDynamoDbAsyncClient()
    val key = primaryKeyMap(entityKey).toJava
    val prefixedDataset = s"prefix_$dataset"
    val daxClient = new RecordingDynamoDbAsyncClient(batchGetHandler = request => {
      request.requestItems().keySet().toScala.toSet shouldBe Set(prefixedDataset)
      completedBatchGet(prefixedDataset, Seq(key), Seq(key))
    })
    val client = new PrefixedDynamoDbAsyncClient(rawClient, "prefix_", _ => Some(daxClient))
    val request = BatchGetItemRequest
      .builder()
      .requestItems(Map(dataset -> KeysAndAttributes.builder().keys(key).build()).toJava)
      .build()

    val response = client.batchGetItem(request).join()

    rawClient.batchGetRequests shouldBe empty
    daxClient.batchGetRequests should have size 1
    response.responses().keySet().toScala.toSet shouldBe Set(dataset)
    response.unprocessedKeys().keySet().toScala.toSet shouldBe Set(dataset)
  }

  private def tiledRequest(startTs: Long, endTs: Long): GetRequest = {
    val tileKey = TilingUtils.buildTileKey(dataset, entityKey, Some(tileSizeMillis), None)
    GetRequest(TilingUtils.serializeTileKey(tileKey), dataset, Some(startTs), Some(endTs))
  }

  private def echoBatchGetItems(request: BatchGetItemRequest): BatchGetItemResponse = {
    val (tableName, keysAndAttributes) = request.requestItems().toScala.head
    completedBatchGet(tableName, keysAndAttributes.keys().toScala, Seq.empty)
  }

  private def completedBatchGet(tableName: String,
                                processedKeys: Seq[util.Map[String, AttributeValue]],
                                unprocessedKeys: Seq[util.Map[String, AttributeValue]]): BatchGetItemResponse = {
    val items = processedKeys.map { key =>
      val item = new util.HashMap[String, AttributeValue](key)
      item.put("valueBytes", AttributeValue.builder().b(SdkBytes.fromUtf8String("value")).build())
      item
    }
    val builder = BatchGetItemResponse.builder().responses(Map(tableName -> items.toJava).toJava)
    if (unprocessedKeys.nonEmpty) {
      builder.unprocessedKeys(
        Map(tableName -> KeysAndAttributes.builder().keys(unprocessedKeys.toList.toJava).build()).toJava)
    }
    builder.build()
  }

  private def completedGetItem(value: String): GetItemResponse =
    GetItemResponse
      .builder()
      .item(
        Map(
          "valueBytes" -> AttributeValue.builder().b(SdkBytes.fromUtf8String(value)).build()
        ).toJava)
      .build()

  private def awaitCondition(condition: => Boolean): Unit = {
    val deadline = 2.seconds.fromNow
    while (!condition && deadline.hasTimeLeft()) Thread.sleep(10)
    condition shouldBe true
  }

  private class RecordingDynamoDbAsyncClient(
      getItemHandler: GetItemRequest => GetItemResponse = _ => GetItemResponse.builder().build(),
      batchGetHandler: BatchGetItemRequest => BatchGetItemResponse = _ => BatchGetItemResponse.builder().build(),
      queryHandler: QueryRequest => QueryResponse = _ => QueryResponse.builder().build(),
      putItemHandler: PutItemRequest => PutItemResponse = _ => PutItemResponse.builder().build(),
      batchGetFutureHandler: Option[BatchGetItemRequest => CompletableFuture[BatchGetItemResponse]] = None)
      extends DynamoDbAsyncClient {

    val getItemRequests: ArrayBuffer[GetItemRequest] = ArrayBuffer.empty
    val batchGetRequests: ArrayBuffer[BatchGetItemRequest] = ArrayBuffer.empty
    val queryRequests: ArrayBuffer[QueryRequest] = ArrayBuffer.empty
    val putItemRequests: ArrayBuffer[PutItemRequest] = ArrayBuffer.empty

    override def getItem(request: GetItemRequest): CompletableFuture[GetItemResponse] = {
      getItemRequests += request
      CompletableFuture.completedFuture(getItemHandler(request))
    }

    override def batchGetItem(request: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
      batchGetRequests += request
      batchGetFutureHandler
        .map(handler => handler(request))
        .getOrElse(CompletableFuture.completedFuture(batchGetHandler(request)))
    }

    override def query(request: QueryRequest): CompletableFuture[QueryResponse] = {
      queryRequests += request
      CompletableFuture.completedFuture(queryHandler(request))
    }

    override def putItem(request: PutItemRequest): CompletableFuture[PutItemResponse] = {
      putItemRequests += request
      CompletableFuture.completedFuture(putItemHandler(request))
    }

    override def serviceName(): String = "dynamodb"

    override def close(): Unit = ()
  }
}
