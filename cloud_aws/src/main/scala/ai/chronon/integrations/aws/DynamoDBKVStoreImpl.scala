package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{
  ContinuationKey,
  KvDaxEndpointArg,
  KvEnableDaxArg,
  KvEnableTtlArg,
  KvReplicaRegionsArg,
  KvTablePrefixArg,
  KvUploadBatchTableGCAgeDaysKey,
  KvUploadTimeoutMsKey,
  ListLimit
}
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Constants, PartitionSpec, TilingUtils}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.spark.{IonPathConfig, IonWriter}
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.nio.charset.Charset
import java.time.{Duration, Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util
import java.util.concurrent.{CompletableFuture, CompletionException, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class DynamoDBKVStoreImpl(rawDynamoDbClient: DynamoDbAsyncClient,
                          conf: Map[String, String] = Map.empty,
                          daxClientProvider: Option[String => DynamoDbAsyncClient])
    extends KVStore {
  import DynamoDBKVStoreConstants._

  def this(rawDynamoDbClient: DynamoDbAsyncClient, conf: Map[String, String]) =
    this(rawDynamoDbClient, conf, None)

  def this(rawDynamoDbClient: DynamoDbAsyncClient) =
    this(rawDynamoDbClient, Map.empty, None)

  protected val enableTtl: Boolean = conf.getOrElse(KvEnableTtlArg, "true").toBoolean
  private[aws] val enableDax: Boolean = conf.getOrElse(KvEnableDaxArg, "false").toBoolean
  private[aws] val configuredDaxEndpoint: Option[String] =
    conf
      .get(KvDaxEndpointArg)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(_ => enableDax)
      .map(validateDaxEndpoint)
  private[aws] def daxEnabled: Boolean = configuredDaxEndpoint.nonEmpty

  private val tablePrefix = conf.getOrElse(KvTablePrefixArg, "")

  private val replicaRegions: List[String] =
    conf
      .get(KvReplicaRegionsArg)
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .getOrElse(List.empty)

  private val cacheEndpointsByTable = TrieMap.empty[String, Option[String]]
  private val physicalTableByDataset = TrieMap.empty[String, String]
  private val batchGetRequestLimiter = new AsyncRequestLimiter(BatchGetMaxConcurrentRequests)
  private lazy val daxClientForEndpoint = daxClientProvider.getOrElse(AwsApiImpl.cachedDaxClientProvider(conf))

  // Wrap the client to automatically prefix all table names. Data-plane calls can route to DAX;
  // metadata and control-plane calls stay on the raw DynamoDB client.
  private[aws] lazy val dynamoDbClient: PrefixedDynamoDbAsyncClient = {
    logger.info(
      s"Using: table prefix: '$tablePrefix' (prefix will be added to all table names used by this KVStore); enableTtl: $enableTtl; enableDax: $enableDax")
    val dataDelegate = if (enableDax) cacheEndpointForTable _ else (_: String) => None
    new PrefixedDynamoDbAsyncClient(rawDynamoDbClient, tablePrefix, dataDelegateForTableName = dataDelegate)
  }

  if (enableDax && configuredDaxEndpoint.isEmpty) {
    logger.info(
      s"$KvEnableDaxArg is true without a global $KvDaxEndpointArg; per-table registry endpoints will still be used")
  }

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb")

  // TTLCache: resolves logical batch dataset names to physical date-suffixed table names
  private val batchTableCache: TTLCache[String, BatchTableInfo] = new TTLCache[String, BatchTableInfo](
    f = { dataset =>
      val keyMap = Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(dataset.getBytes)).build)
      val request = GetItemRequest.builder
        .tableName(batchTableRegistry)
        .key(keyMap.toJava)
        .build

      val item = dynamoDbClient.getItem(request).join().item().toScala
      val parsedTableInfo = item
        .get("valueBytes")
        .map(v => batchTableInfoFromRegistryValue(new String(v.b().asByteArray())))
        .getOrElse(BatchTableInfo(dataset, daxEndpoint = None))
      val resolvedEndpoint =
        if (enableDax) parsedTableInfo.daxEndpoint.orElse(configuredDaxEndpoint) else None
      recordCacheEndpoint(dataset, parsedTableInfo.copy(daxEndpoint = resolvedEndpoint))
    },
    contextBuilder = { _ => metricsContext.withSuffix("batch_table_cache") }
  )

  private[aws] def resolveBatchTableInfo(dataset: String): BatchTableInfo = {
    if (dataset.endsWith(batchSuffix)) {
      batchTableCache.refresh(dataset)
    } else {
      val daxEndpoint =
        if (!enableDax || isMetadataDataset(dataset)) None
        else if (isStreamingTable(dataset)) {
          val siblingBatchDataset = dataset.stripSuffix(streamingSuffix) + batchSuffix
          batchTableCache.refresh(siblingBatchDataset).daxEndpoint.orElse(configuredDaxEndpoint)
        } else configuredDaxEndpoint
      recordCacheEndpoint(dataset, BatchTableInfo(dataset, daxEndpoint))
    }
  }

  private[aws] def resolveTableName(dataset: String): String =
    resolveBatchTableInfo(dataset).physicalTableName

  private[aws] def batchTableRegistryValue(physicalTableName: String): String =
    registryValueForBatchTable(physicalTableName, configuredDaxEndpoint)

  private[aws] def isMetadataDataset(dataset: String): Boolean =
    dataset == Constants.MetadataDataset || dataset == batchTableRegistry

  private[aws] def isCacheEligible(dataset: String): Boolean =
    resolveBatchTableInfo(dataset).daxEndpoint.nonEmpty && !isMetadataDataset(dataset)

  private def recordCacheEndpoint(dataset: String, tableInfo: BatchTableInfo): BatchTableInfo = {
    if (enableDax) {
      physicalTableByDataset
        .put(dataset, tableInfo.physicalTableName)
        .filterNot(_ == tableInfo.physicalTableName)
        .foreach(cacheEndpointsByTable.remove)
      cacheEndpointsByTable.put(tableInfo.physicalTableName, tableInfo.daxEndpoint)
    }
    tableInfo
  }

  private def cacheEndpointForTable(tableName: String): Option[DynamoDbAsyncClient] =
    if (isMetadataDataset(tableName)) None
    else {
      // Streaming writers are long-lived. Resolve through the TTL-backed sibling registry on every write
      // so an endpoint rotation cannot leave Flink writing through an old DAX cluster indefinitely.
      val endpoint =
        if (isStreamingTable(tableName)) resolveBatchTableInfo(tableName).daxEndpoint
        else
          cacheEndpointsByTable.get(tableName) match {
            case Some(cachedEndpoint) => cachedEndpoint
            case None                 => resolveBatchTableInfo(tableName).daxEndpoint
          }
      endpoint.map(daxClientForEndpoint)
    }

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  private def tableExists(dataset: String): Boolean = {
    val request = DescribeTableRequest.builder.tableName(dataset).build
    try {
      dynamoDbClient.describeTable(request).join()
      true
    } catch {
      case _: ResourceNotFoundException                                                 => false
      case e: CompletionException if e.getCause.isInstanceOf[ResourceNotFoundException] => false
    }
  }

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    if (tableExists(dataset)) {
      logger.info(s"DynamoDB table $dataset already exists, skipping creation")
      return
    }

    val maybeSortKeys = props.get(isTimedSorted) match {
      case Some(value: String) if value.toLowerCase == "true" => Some(sortKeyColumn)
      case Some(value: Boolean) if value                      => Some(sortKeyColumn)
      case _ if isStreamingTable(dataset)                     => Some(sortKeyColumn)
      case _                                                  => None
    }

    val keyAttributes =
      Seq(AttributeDefinition.builder.attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build) ++
        maybeSortKeys.map(k => AttributeDefinition.builder.attributeName(k).attributeType(ScalarAttributeType.N).build)

    val keySchema =
      Seq(KeySchemaElement.builder.attributeName(partitionKeyColumn).keyType(KeyType.HASH).build) ++
        maybeSortKeys.map(p => KeySchemaElement.builder.attributeName(p).keyType(KeyType.RANGE).build)

    val request =
      CreateTableRequest.builder
        .attributeDefinitions(keyAttributes.toList.toJava)
        .keySchema(keySchema.toList.toJava)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .tableName(dataset)
        .build

    logger.info(s"Triggering creation of DynamoDb table: $dataset with prefix '$tablePrefix' added later")
    try {
      dynamoDbClient.createTable(request).join()
      val waiterResponse = dynamoDbClient.waitUntilTableExists(dataset).join()
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()

      val tableDescription = waiterResponse.matched().response().get().table()
      logger.info(s"Table created successfully! Details: \n${tableDescription.toString}")

      if (enableTtl) {
        val ttlSpec = TimeToLiveSpecification.builder
          .enabled(true)
          .attributeName("ttl")
          .build
        val ttlRequest = UpdateTimeToLiveRequest.builder
          .tableName(dataset)
          .timeToLiveSpecification(ttlSpec)
          .build
        dynamoDbClient.updateTimeToLive(ttlRequest).join()
        logger.info(s"TTL enabled on table: $dataset with attribute 'ttl'")
      }

      addReplicaRegions(dataset)

      metricsContext.increment("create.successes")
    } catch {
      case _: ResourceInUseException =>
        logger.info(s"Table: $dataset already exists")
      case e: CompletionException if e.getCause.isInstanceOf[ResourceInUseException] =>
        logger.info(s"Table: $dataset already exists")
      case e: Exception =>
        logger.error(s"Error creating Dynamodb table: $dataset", e)
        metricsContext.increment("create.failures")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // partition our requests into pure get style requests (where we only have key lookup)
    // and query requests (we want to query a range based on afterTsMillis -> endTsMillis or now() )
    val (getLookups, queryLookups) = requests.partition(r => r.startTsMillis.isEmpty)
    val getItemResults = doGetLookups(getLookups)
    val aggregatedQueryResults = doQueryLookups(queryLookups)

    Future.sequence(getItemResults ++ aggregatedQueryResults)
  }

  protected def doGetLookups(getLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    val getItemCompletables = getLookups.map { req =>
      val keyAttributeMap = primaryKeyMap(req.keyBytes)
      val tableInfo = resolveBatchTableInfo(req.dataset)
      val getItemReq = GetItemRequest.builder.key(keyAttributeMap.toJava).tableName(tableInfo.physicalTableName).build
      val startTs = System.currentTimeMillis()
      (req, dynamoDbClient.getItem(getItemReq), startTs)
    }

    // timestamp to use for all get responses when the underlying tables don't have a ts field
    val defaultTimestamp = Instant.now().toEpochMilli

    val getItemResults = getItemCompletables.map { case (req, completableFuture, startTs) =>
      handleDynamoDbOperation(metricsContext.withSuffix("multiget"), req.dataset, startTs)(completableFuture)
        .transform {
          case Success(response) =>
            val resultValue = extractTimedValues(List(response.item()).toJava, defaultTimestamp)
            Success(GetResponse(req, resultValue))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
    }
    getItemResults
  }

  protected def queryPartition(dataset: String,
                               partitionKeyBytes: Array[Byte],
                               startTs: Long,
                               endTs: Option[Long]): Future[QueryResponse] = {
    val queryRequest = buildTimeRangeQuery(dataset, partitionKeyBytes, startTs, endTs)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      dynamoDbClient.query(queryRequest)
    )
  }

  protected def queryPartitionOnly(dataset: String, partitionKeyBytes: Array[Byte]): Future[QueryResponse] = {
    val queryRequest = buildPartitionOnlyQuery(dataset, partitionKeyBytes)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      dynamoDbClient.query(queryRequest)
    )
  }

  protected def batchGetTimeRange(dataset: String,
                                  baseKeyBytes: Array[Byte],
                                  startTs: Long,
                                  endTs: Long,
                                  tileSizeMillis: Long): Future[Seq[TimedValue]] = {
    val keyBatches = timeSeriesGetKeyIterator(baseKeyBytes, startTs, endTs, tileSizeMillis).grouped(BatchGetMaxKeys)
    batchGetItems(dataset, keyBatches).map { items =>
      extractTimedValues(items.toList.toJava, Instant.now().toEpochMilli).get.sortBy(_.millis)
    }
  }

  private def batchGetItems(
      dataset: String,
      keyBatches: Iterator[Seq[Map[String, AttributeValue]]]): Future[Seq[util.Map[String, AttributeValue]]] = {
    def nextWave(
        accumulated: Vector[util.Map[String, AttributeValue]]): Future[Vector[util.Map[String, AttributeValue]]] = {
      val wave = keyBatches.take(BatchGetMaxConcurrentRequests).toSeq
      if (wave.isEmpty) Future.successful(accumulated)
      else {
        Future
          .sequence(wave.map(batch => batchGetChunk(dataset, batch, attempt = 0)))
          .flatMap(results => nextWave(accumulated ++ results.flatten))
      }
    }

    nextWave(Vector.empty)
  }

  private def batchGetChunk(dataset: String,
                            keys: Seq[Map[String, AttributeValue]],
                            attempt: Int): Future[Seq[util.Map[String, AttributeValue]]] = {
    val keysAndAttributes = KeysAndAttributes.builder().keys(keys.map(_.toJava).toList.toJava).build()
    val request = BatchGetItemRequest.builder().requestItems(Map(dataset -> keysAndAttributes).toJava).build()

    batchGetRequestLimiter
      .withPermit {
        val callStartTs = System.currentTimeMillis()
        handleDynamoDbOperation(metricsContext.withSuffix("batch_get"), dataset, callStartTs)(
          dynamoDbClient.batchGetItem(request)
        )
      }
      .flatMap { response =>
        val items = Option(response.responses())
          .flatMap(responses => responses.toScala.get(dataset))
          .map(_.toScala)
          .getOrElse(Seq.empty)
        val unprocessedKeys = Option(response.unprocessedKeys())
          .flatMap(unprocessed => unprocessed.toScala.get(dataset))
          .map(_.keys().toScala.map(_.toScala.toMap))
          .getOrElse(Seq.empty)

        if (unprocessedKeys.isEmpty) {
          Future.successful(items)
        } else if (attempt >= BatchGetMaxRetries) {
          Future.failed(new RuntimeException(
            s"DynamoDB BatchGetItem left ${unprocessedKeys.size} keys unprocessed for $dataset after $attempt retries"))
        } else {
          delayedFuture(BatchGetRetryBaseDelayMillis * (1L << attempt))
            .flatMap(_ => batchGetChunk(dataset, unprocessedKeys, attempt + 1))
            .map(items ++ _)
        }
      }
  }

  private def delayedFuture(delayMillis: Long): Future[Unit] = {
    val result = new CompletableFuture[Unit]()
    CompletableFuture
      .delayedExecutor(delayMillis, TimeUnit.MILLISECONDS)
      .execute(new Runnable {
        override def run(): Unit = result.complete(())
      })
    FutureConverters.toScala(result)
  }

  private def queryTimeRange(dataset: String,
                             partitionKeys: Seq[Array[Byte]],
                             startTs: Long,
                             endTs: Long): Future[Seq[TimedValue]] = {
    val defaultTimestamp = Instant.now().toEpochMilli
    Future
      .sequence(partitionKeys.map(partitionKey => queryPartition(dataset, partitionKey, startTs, Some(endTs))))
      .map { responses =>
        responses
          .flatMap(response => extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty))
          .sortBy(_.millis)
      }
  }

  protected def doQueryLookups(queryLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    queryLookups.map { req =>
      val tableInfo = resolveBatchTableInfo(req.dataset)
      val tileComponents = extractTileKeyComponents(req.keyBytes)
      val endTs = req.endTsMillis.getOrElse(System.currentTimeMillis())

      val timedValues =
        if (tableInfo.daxEndpoint.nonEmpty && isStreamingTable(req.dataset) && tileComponents.tileSizeMillis > 0) {
          batchGetTimeRange(tableInfo.physicalTableName,
                            tileComponents.baseKeyBytes,
                            req.startTsMillis.get,
                            endTs,
                            tileComponents.tileSizeMillis)
        } else {
          val partitionKeys = generateTimeSeriesKeys(
            tileComponents.baseKeyBytes,
            req.startTsMillis.get,
            endTs,
            tileComponents.tileSizeMillis
          )
          queryTimeRange(tableInfo.physicalTableName, partitionKeys, req.startTsMillis.get, endTs)
        }

      timedValues.transform {
        case Success(values) => Success(GetResponse(req, Success(values)))
        case Failure(e)      => Success(GetResponse(req, Failure(e)))
      }
    }
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => 100
    }

    val maybeExclusiveStartKey = request.props.get(ContinuationKey)
    val maybeExclusiveStartKeyAttribute = maybeExclusiveStartKey.map { k =>
      AttributeValue.builder.b(SdkBytes.fromByteArray(k.asInstanceOf[Array[Byte]])).build
    }

    val scanBuilder = ScanRequest.builder.tableName(request.dataset).limit(listLimit)
    val scanRequest = maybeExclusiveStartKeyAttribute match {
      case Some(value) => scanBuilder.exclusiveStartKey(Map(partitionKeyColumn -> value).toJava).build
      case _           => scanBuilder.build
    }

    val startTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("list"), request.dataset, startTs)(
      dynamoDbClient.scan(scanRequest)
    ).map { scanResponse =>
      val resultElements = extractListValues(scanResponse)
      val noPagesLeftResponse = ListResponse(request, resultElements, Map.empty)
      if (scanResponse.hasLastEvaluatedKey) {

        val lastEvalKey = scanResponse.lastEvaluatedKey().toScala.get(partitionKeyColumn)
        lastEvalKey match {
          case Some(av) => ListResponse(request, resultElements, Map(ContinuationKey -> av.b().asByteArray()))
          case _        => noPagesLeftResponse
        }
      } else {
        noPagesLeftResponse
      }
    }.recover { case e: Exception =>
      ListResponse(request, Failure(e), Map.empty)
    }
  }

  // Dynamo has restrictions on the number of requests per batch (and the payload size) as well as some partial
  // success behavior on batch writes which necessitates a bit more logic on our end to tie things together.
  // To keep things simple for now, we implement the multiput as a sequence of put calls.
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    val futureResponses = keyValueDatasets.map { req =>
      val (actualKeyBytes, actualTimestamp) = if (isStreamingTable(req.dataset)) {
        // For streaming tables, unwrap TileKey to use entity key + tileSizeMs as partition key
        // and tileStartTs as sort key. Including tileSizeMs in the key supports tile layering.
        val tileComponents = extractTileKeyComponents(req.keyBytes)
        val timestamp = tileComponents.tileStartTimestampMillis
        val tiledKey = buildKeyWithTileSize(tileComponents.baseKeyBytes, timestamp, tileComponents.tileSizeMillis)
        (tiledKey, timestamp)
      } else {
        val timestampInPutRequest = req.tsMillis.getOrElse(System.currentTimeMillis())
        (req.keyBytes, timestampInPutRequest)
      }

      val attributeMap: Map[String, AttributeValue] = buildAttributeMap(actualKeyBytes, req.valueBytes)
      val tsMap = Map(sortKeyColumn -> AttributeValue.builder.n(actualTimestamp.toString).build)
      val ttlMap = if (enableTtl) {
        val ttlSeconds = (System.currentTimeMillis() / 1000).toInt + DataTTLSeconds
        Map("ttl" -> AttributeValue.builder.n(ttlSeconds.toString).build)
      } else Map.empty[String, AttributeValue]

      val putItemReq =
        PutItemRequest.builder.tableName(req.dataset).item((attributeMap ++ tsMap ++ ttlMap).toJava).build()
      val startTs = System.currentTimeMillis()
      handleDynamoDbOperation(metricsContext.withSuffix("multiput"), req.dataset, startTs)(
        dynamoDbClient.putItem(putItemReq)
      ).transform {
        case Success(_) => Success(true)
        case Failure(_) => Success(false)
      }
    }
    Future.sequence(futureResponses)
  }

  /** Bulk loads data from S3 Ion files into DynamoDB using the ImportTable API.
    *
    * The Ion files are expected to have been written by IonWriter during GroupByUpload.
    * The S3 location is determined by IonWriter.resolveS3Location using:
    *   - Root path from config: spark.chronon.table_write.upload.root_path
    *   - Dataset name: sourceOfflineTable (e.g., namespace.groupby_v1__upload)
    *   - Partition column and value: ds={partition}
    *
    * Full path: s3://{spark.chronon.table_write.upload.root_path}/{sourceOfflineTable}/ds={partition}/
    *
    * Creates a date-suffixed physical table (e.g. MY_GROUPBY_BATCH_2026_02_17) and registers the
    * mapping from logical dataset name to physical table in CHRONON_BATCH_TABLE_REGISTRY.
    */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val rootPath = conf.get(IonPathConfig.UploadLocationKey)
    val partitionColumn = conf.getOrElse(IonPathConfig.PartitionColumnKey, IonPathConfig.DefaultPartitionColumn)

    // Use shared IonWriter path resolution to ensure consistency between producer and consumer
    val path = IonWriter.resolvePartitionPath(sourceOfflineTable, partitionColumn, partition, rootPath)
    val s3Source = toS3BucketSource(path)
    val logicalTableName = destinationOnlineDataSet
    val timestamp = Instant.now().toEpochMilli
    val physicalTableName =
      logicalTableName.sanitize.toUpperCase + "_" + partition.replace("-", "_") + "_" + timestamp
    logger.info(
      s"Starting DynamoDB import for table: $physicalTableName (logical: $logicalTableName) from S3: $s3Source with prefix '$tablePrefix' added later")

    val tableParams = TableCreationParameters
      .builder()
      .tableName(physicalTableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(partitionKeyColumn).keyType(KeyType.HASH).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    val importRequest = ImportTableRequest
      .builder()
      .s3BucketSource(s3Source)
      .inputFormat(InputFormat.ION)
      .inputCompressionType(InputCompressionType.NONE)
      .tableCreationParameters(tableParams)
      .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
      .build()

    try {
      val startTs = System.currentTimeMillis()
      val importResponse = dynamoDbClient.importTable(importRequest).join()
      val importArn = importResponse.importTableDescription().importArn()

      logger.info(s"DynamoDB import initiated with ARN: $importArn for table: $physicalTableName")

      waitForImportCompletion(importArn, physicalTableName, configuredImportTimeout)

      // ImportTable API does not support TTL configuration; must be applied after import completes
      if (enableTtl) {
        val ttlSpec = TimeToLiveSpecification.builder.enabled(true).attributeName("ttl").build
        val ttlRequest = UpdateTimeToLiveRequest.builder
          .tableName(physicalTableName)
          .timeToLiveSpecification(ttlSpec)
          .build
        dynamoDbClient.updateTimeToLive(ttlRequest).join()
        logger.info(s"TTL enabled on imported table: $physicalTableName")
      }

      addReplicaRegions(physicalTableName)

      // Register the physical table name in the batch table registry
      create(batchTableRegistry)
      val registryKey = logicalTableName.sanitize.toUpperCase + batchSuffix
      val registryValue = batchTableRegistryValue(physicalTableName)
      Await.result(
        multiPut(Seq(KVStore.PutRequest(registryKey.getBytes, registryValue.getBytes, batchTableRegistry))),
        30.seconds
      )
      logger.info(s"Registry updated: $registryKey -> $registryValue")

      gcOldBatchTables(logicalTableName)

      val duration = System.currentTimeMillis() - startTs
      logger.info(s"DynamoDB import completed for table: $physicalTableName in ${duration}ms")
      metricsContext.increment("bulkPut.successes")
      metricsContext.distribution("bulkPut.latency", duration)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to import data to DynamoDB table: $physicalTableName", e)
        metricsContext.increment("bulkPut.failures")
        throw e
    }
  }

  /** Deletes batch tables for the given logical name that are older than the configured GC age, up to
    * BatchTableGCMaxDelete at a time. Failures are swallowed so GC never blocks bulkPut.
    */
  private[aws] def gcOldBatchTables(logicalTableName: String): Unit = {
    if (!enableTtl) return
    try {
      val prefix = logicalTableName.sanitize.toUpperCase + "_"
      val cleanupDays = conf.get(KvUploadBatchTableGCAgeDaysKey).map(_.toLong).getOrElse(BatchTableGCAgeDays.toLong)
      val cutoff = LocalDate.now().minusDays(cleanupDays)

      // DynamoDB listTables returns names in ASCII order. By starting pagination at the prefix
      // (exclusive), we land right at the first matching table and stop as soon as names diverge —
      // avoiding a full scan of all tables.
      val allMatchingTables = mutable.Set.empty[String]
      // prefix always ends with "_" (e.g. "MY_GROUPBY_BATCH_"). Dropping it gives "MY_GROUPBY_BATCH",
      // which sorts before all "MY_GROUPBY_BATCH_..." names, so DynamoDB starts returning
      // matches from the first page.
      var exclusiveStart: Option[String] = Some(prefix.dropRight(1))
      var hasMore = true
      while (hasMore) {
        val reqBuilder = ListTablesRequest.builder.limit(100)
        exclusiveStart.foreach(reqBuilder.exclusiveStartTableName)
        val resp = dynamoDbClient.listTables(reqBuilder.build()).join()
        val page = resp.tableNames().toScala
        val matching = page.takeWhile(_.startsWith(prefix))
        allMatchingTables ++= matching
        // Stop if we've gone past the prefix range or there are no more pages
        if (matching.size < page.size || resp.lastEvaluatedTableName() == null) {
          hasMore = false
        } else {
          exclusiveStart = Some(resp.lastEvaluatedTableName())
        }
      }

      // Parse dates from table names: {PREFIX}_{YYYY_MM_DD}_{timestamp}
      val oldTables = allMatchingTables.flatMap { tableName =>
        val suffix = tableName.stripPrefix(prefix) // e.g. "2026_02_17_1708192000000"
        val parts = suffix.split("_")
        // date is first 3 parts: YYYY, MM, DD
        if (parts.length >= 4) {
          val dateStr = s"${parts(0)}_${parts(1)}_${parts(2)}"
          try {
            val tableDate = LocalDate.parse(dateStr, BatchTableDateFormatter)
            if (tableDate.isBefore(cutoff)) Some(tableName) else None
          } catch {
            case e: DateTimeParseException =>
              logger.warn(s"Could not parse date from batch table name '$tableName': ${e.getMessage}")
              None
          }
        } else None
      }

      val toDelete = oldTables.toSeq.sortBy(identity).take(BatchTableGCMaxDelete)
      logger.info(
        s"Batch table GC for $logicalTableName: found ${oldTables.size} tables older than $cleanupDays days, deleting ${toDelete.size}")

      toDelete.foreach { tableName =>
        try {
          dynamoDbClient
            .deleteTable(DeleteTableRequest.builder.tableName(tableName).build())
            .join()
          cacheEndpointsByTable.remove(tableName)
          logger.info(s"Deleted old batch table: $tableName")
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to delete old batch table: $tableName", e)
        }
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Batch table GC failed for $logicalTableName, skipping", e)
    }
  }

  /** Converts a Hadoop Path to an S3BucketSource for DynamoDB ImportTable. */
  private def toS3BucketSource(path: org.apache.hadoop.fs.Path): S3BucketSource = {
    val uri = path.toUri
    S3BucketSource
      .builder()
      .s3Bucket(uri.getHost)
      .s3KeyPrefix(uri.getPath.stripPrefix("/") + "/")
      .build()
  }

  private[aws] def configuredImportTimeout: Duration =
    conf
      .get(KvUploadTimeoutMsKey)
      .orElse(conf.get(IonPathConfig.IonWriterTimeoutKey))
      .map(timeoutMillis => Duration.ofMillis(timeoutMillis.toLong))
      .getOrElse(DynamoImportDefaultTimeout)

  private def waitForImportCompletion(importArn: String, tableName: String, timeout: Duration): Unit = {
    val maxWaitTimeMs = timeout.toMillis
    val pollIntervalMs = 10 * 1000L // 10 seconds
    val startTime = System.currentTimeMillis()

    var status: ImportStatus = ImportStatus.IN_PROGRESS
    var lastDescription: ImportTableDescription = null
    while (status == ImportStatus.IN_PROGRESS && (System.currentTimeMillis() - startTime) < maxWaitTimeMs) {
      Thread.sleep(pollIntervalMs)

      try {
        val describeRequest = DescribeImportRequest
          .builder()
          .importArn(importArn)
          .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
          .build()
        val describeResponse = dynamoDbClient.describeImport(describeRequest).join()
        lastDescription = describeResponse.importTableDescription()
        status = lastDescription.importStatus()

        val elapsed = (System.currentTimeMillis() - startTime) / 1000
        logger.info(
          s"DynamoDB import status for $tableName: $status " +
            s"(${elapsed}s elapsed, processed: ${lastDescription.processedItemCount()} items, " +
            s"imported: ${lastDescription.importedItemCount()} items, " +
            s"errors: ${lastDescription.errorCount()})")
      } catch {
        case e: Exception =>
          logger.error(s"Error polling import status for $tableName", e)
          throw e
      }
    }

    status match {
      case ImportStatus.COMPLETED =>
        logger.info(
          s"DynamoDB import completed successfully for table: $tableName " +
            s"(imported: ${lastDescription.importedItemCount()} items, errors: ${lastDescription.errorCount()})")
      case ImportStatus.FAILED | ImportStatus.CANCELLED =>
        val diagnostics =
          s"""DynamoDB import failed for table: $tableName
             |  Status: $status
             |  Failure Code: ${lastDescription.failureCode()}
             |  Failure Message: ${lastDescription.failureMessage()}
             |  Error Count: ${lastDescription.errorCount()}
             |  Processed Items: ${lastDescription.processedItemCount()}
             |  Imported Items: ${lastDescription.importedItemCount()}
             |  Import ARN: $importArn""".stripMargin
        logger.error(diagnostics)
        throw new RuntimeException(diagnostics)
      case ImportStatus.IN_PROGRESS =>
        throw new RuntimeException(s"DynamoDB import timed out after $timeout for table: $tableName")
      case _ =>
        logger.warn(s"Unknown import status: $status for table: $tableName")
    }
  }

  protected def handleDynamoDbOperation[T](context: Context, dataset: String, startTs: Long)(
      completableFuture: CompletableFuture[T]): Future[T] = {
    FutureConverters.toScala(completableFuture).transform {
      case Success(result) =>
        context.distribution("latency", System.currentTimeMillis() - startTs)
        Success(result)
      case Failure(exception) =>
        exception match {
          case e: ProvisionedThroughputExceededException =>
            logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", e)
            context.increment("iops_error")
            Failure(e)
          case e: ResourceNotFoundException =>
            logger.error(s"Unable to trigger operation on $dataset as its not found", e)
            context.increment("missing_table")
            Failure(e)
          case e: CompletionException =>
            e.getCause match {
              case ce: ProvisionedThroughputExceededException =>
                logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", ce)
                context.increment("iops_error")
                Failure(ce)
              case ce: ResourceNotFoundException =>
                logger.error(s"Unable to trigger operation on $dataset as its not found", ce)
                context.increment("missing_table")
                Failure(ce)
              case _ =>
                logger.error("Error interacting with DynamoDB", e.getCause)
                context.increment("dynamodb_error")
                Failure(e.getCause)
            }
          case e: Exception =>
            logger.error("Error interacting with DynamoDB", e)
            context.increment("dynamodb_error")
            Failure(e)
        }
    }
  }

  protected def extractTimedValues(ddbResponseList: util.List[util.Map[String, AttributeValue]],
                                   defaultTimestamp: Long): Try[Seq[TimedValue]] = {
    Try {
      ddbResponseList.toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())
        if (valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing valueBytes")

        val timestamp = responseMap.get(sortKeyColumn).map(v => v.n().toLong).getOrElse(defaultTimestamp)
        TimedValue(valueBytes.get, timestamp)
      }
    }
  }

  private def extractListValues(scanResponse: ScanResponse): Try[Seq[ListValue]] = {
    Try {
      scanResponse.items().toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val keyBytes = responseMap.get("keyBytes").map(v => v.b().asByteArray())
        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())

        if (keyBytes.isEmpty || valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing key / valueBytes")
        ListValue(keyBytes.get, valueBytes.get)
      }
    }
  }

  // Global Tables v2: called after a table is active to add read replicas in additional regions.
  // Failure is non-fatal — replicas are best-effort for read locality.
  // Uses a per-request 30s timeout override because UpdateTable is a control-plane operation
  // that is much slower than data-plane calls and would otherwise hit the shared 3s client timeout.
  protected def addReplicaRegions(tableName: String): Unit = {
    if (replicaRegions.isEmpty) return
    try {
      val replicaUpdates = replicaRegions.map { region =>
        ReplicationGroupUpdate
          .builder()
          .create(CreateReplicationGroupMemberAction.builder().regionName(region).build())
          .build()
      }
      val requestOverride = AwsRequestOverrideConfiguration
        .builder()
        .apiCallTimeout(Duration.ofSeconds(10))
        .apiCallAttemptTimeout(Duration.ofSeconds(10))
        .build()
      val updateRequest = UpdateTableRequest
        .builder()
        .tableName(tableName)
        .replicaUpdates(replicaUpdates.toList.toJava)
        .overrideConfiguration(requestOverride)
        .build()
      dynamoDbClient.updateTable(updateRequest).join()
      logger.info(s"Global Table replicas added for '$tableName' in regions: ${replicaRegions.mkString(", ")}")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to add replica regions for table '$tableName' — replicas are best-effort", e)
    }
  }
}

object DynamoDBKVStoreConstants {
  val batchTableRegistry: String = "CHRONON_BATCH_TABLE_REGISTRY"
  val batchSuffix = "_BATCH"
  val streamingSuffix = "_STREAMING"
  val DaxTableDelimiter = "@"
  val DaxEndpointPrefix = "dax://"
  private val DaxRegistryValuePattern = s"^(${DaxEndpointPrefix}[^${DaxTableDelimiter}]+)${DaxTableDelimiter}(.+)$$".r

  case class BatchTableInfo(physicalTableName: String, daxEndpoint: Option[String])

  private[aws] final class AsyncRequestLimiter(maxPermits: Int) {
    require(maxPermits > 0, s"maxPermits must be positive, found $maxPermits")

    private var availablePermits = maxPermits
    private val waiters = mutable.Queue.empty[Promise[Unit]]

    private def acquire(): Future[Unit] = synchronized {
      if (availablePermits > 0) {
        availablePermits -= 1
        Future.successful(())
      } else {
        val waiter = Promise[Unit]()
        waiters.enqueue(waiter)
        waiter.future
      }
    }

    private def release(): Unit = {
      val nextWaiter = synchronized {
        if (waiters.nonEmpty) Some(waiters.dequeue())
        else {
          availablePermits += 1
          None
        }
      }
      nextWaiter.foreach(_.success(()))
    }

    def withPermit[T](operation: => Future[T])(implicit executionContext: ExecutionContext): Future[T] =
      acquire().flatMap { _ =>
        try operation.andThen { case _ => release() }
        catch {
          case NonFatal(exception) =>
            release()
            Future.failed(exception)
        }
      }
  }

  def validateDaxEndpoint(endpoint: String): String = {
    require(
      endpoint.startsWith(DaxEndpointPrefix) && endpoint.length > DaxEndpointPrefix.length,
      s"DAX endpoint must start with '$DaxEndpointPrefix' and include a host"
    )
    require(!endpoint.contains(DaxTableDelimiter),
            s"DAX endpoint must not contain the registry delimiter '$DaxTableDelimiter'")
    endpoint
  }

  def registryValueForBatchTable(physicalTableName: String, daxEndpoint: Option[String] = None): String =
    daxEndpoint
      .filter(_.nonEmpty)
      .map(endpoint => s"$endpoint$DaxTableDelimiter$physicalTableName")
      .getOrElse(physicalTableName)

  def batchTableInfoFromRegistryValue(value: String): BatchTableInfo = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    trimmed match {
      case DaxRegistryValuePattern(endpoint, physicalTableName) =>
        BatchTableInfo(physicalTableName, Some(endpoint))
      case _ =>
        BatchTableInfo(trimmed, daxEndpoint = None)
    }
  }

  // Optional field that indicates if this table is meant to be time sorted in Dynamo or not
  val isTimedSorted = "is-time-sorted"

  // Name of the partition key column to use
  val partitionKeyColumn = "keyBytes"

  // Name of the time sort key column to use
  val sortKeyColumn = Constants.TimeColumn

  // Streaming tables use TileKey wrapping for tiled data.
  def isStreamingTable(dataset: String): Boolean = dataset.endsWith(streamingSuffix)

  // Control plane operations (ImportTable, DeleteTable, DescribeImport) are slower than
  // data plane operations (GetItem, PutItem). Use higher timeouts to avoid intermittent failures.
  val ControlPlaneApiOverride: AwsRequestOverrideConfiguration = AwsRequestOverrideConfiguration
    .builder()
    .apiCallTimeout(Duration.ofSeconds(30))
    .apiCallAttemptTimeout(Duration.ofSeconds(10))
    .build()

  case class TileKeyComponents(baseKeyBytes: Array[Byte], tileSizeMillis: Long, tileStartTimestampMillis: Long)

  /** Unwraps a TileKey to extract the entity key for use as DynamoDB partition key.
    *
    * Streaming tables have two serialization layers:
    *   - Outer: Thrift (TileKey struct with dataset, keyBytes, tileSizeMs, tileStartTs)
    *   - Inner: Avro (entity key, e.g. customer_id, stored in TileKey.keyBytes)
    *
    * This method deserializes only the Thrift layer. The returned baseKeyBytes
    * remain Avro-encoded and are used directly as the DynamoDB partition key.
    */
  def extractTileKeyComponents(keyBytes: Array[Byte]): TileKeyComponents = {
    val tileKey = TilingUtils.deserializeTileKey(keyBytes)
    val baseKeyBytes = tileKey.keyBytes.toScala.map(_.toByte).toArray
    val tileSizeMs = tileKey.tileSizeMillis
    val tileStartTs = tileKey.tileStartTimestampMillis
    TileKeyComponents(baseKeyBytes, tileSizeMs, tileStartTs)
  }

  val DataTTLSeconds = 5.days.toSeconds.toInt
  val MillisPerDay = 1.day.toMillis
  val DynamoImportDefaultTimeout: Duration = Duration.ofMinutes(60)

  val BatchTableGCAgeDays = 30
  val BatchTableGCMaxDelete = 10
  val BatchGetMaxKeys = 100
  val BatchGetMaxConcurrentRequests = 4
  val BatchGetMaxRetries = 3
  val BatchGetRetryBaseDelayMillis = 10L
  // Batch table names embed the date with '_' separators (e.g. 2026_04_16) since '-' is not valid in DynamoDB table names
  val BatchTableDateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(PartitionSpec.daily.format.replace("-", "_"))

  def roundToDay(timestampMillis: Long): Long = {
    timestampMillis - (timestampMillis % MillisPerDay)
  }

  // Partition key format: {entity-key}#{dayTs}#{tileSizeMs}
  def buildKeyWithTileSize(baseKeyBytes: Array[Byte], timestampMillis: Long, tileSizeMs: Long): Array[Byte] = {
    val dayTs = roundToDay(timestampMillis)
    baseKeyBytes ++ s"#$dayTs#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  def generateTimeSeriesKeys(baseKeyBytes: Array[Byte],
                             startTs: Long,
                             endTs: Long,
                             tileSizeMs: Long): Seq[Array[Byte]] = {
    val startDay = roundToDay(startTs)
    val endDay = roundToDay(endTs)
    (startDay to endDay by MillisPerDay).map { dayTs =>
      buildKeyWithTileSize(baseKeyBytes, dayTs, tileSizeMs)
    }
  }

  def primaryKeyMap(keyBytes: Array[Byte]): Map[String, AttributeValue] = {
    Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(keyBytes)).build)
  }

  private[aws] def timeSeriesGetKeyIterator(baseKeyBytes: Array[Byte],
                                            startTs: Long,
                                            endTs: Long,
                                            tileSizeMillis: Long): Iterator[Map[String, AttributeValue]] = {
    require(tileSizeMillis > 0, s"Tile size must be positive, found $tileSizeMillis")
    if (endTs < startTs) return Iterator.empty

    val remainder = Math.floorMod(startTs, tileSizeMillis)
    val offsetToFirstTile = if (remainder == 0) 0L else tileSizeMillis - remainder
    if (offsetToFirstTile > 0 && startTs > Long.MaxValue - offsetToFirstTile) return Iterator.empty

    val firstTileTimestamp = startTs + offsetToFirstTile
    new Iterator[Map[String, AttributeValue]] {
      private var nextTileTimestamp = firstTileTimestamp
      private var hasMore = firstTileTimestamp <= endTs

      override def hasNext: Boolean = hasMore

      override def next(): Map[String, AttributeValue] = {
        if (!hasMore) throw new NoSuchElementException("next on empty time-series key iterator")

        val tileTimestamp = nextTileTimestamp
        if (tileTimestamp > Long.MaxValue - tileSizeMillis) {
          hasMore = false
        } else {
          nextTileTimestamp = tileTimestamp + tileSizeMillis
          hasMore = nextTileTimestamp <= endTs
        }

        primaryKeyMap(buildKeyWithTileSize(baseKeyBytes, tileTimestamp, tileSizeMillis)) +
          (sortKeyColumn -> AttributeValue.builder.n(tileTimestamp.toString).build)
      }
    }
  }

  /** Builds exact composite keys for all aligned tiles covered by DynamoDB Query's inclusive time bounds. */
  def buildTimeSeriesGetKeys(baseKeyBytes: Array[Byte],
                             startTs: Long,
                             endTs: Long,
                             tileSizeMillis: Long): Seq[Map[String, AttributeValue]] =
    timeSeriesGetKeyIterator(baseKeyBytes, startTs, endTs, tileSizeMillis).toSeq

  def buildAttributeMap(keyBytes: Array[Byte], valueBytes: Array[Byte]): Map[String, AttributeValue] = {
    primaryKeyMap(keyBytes) ++
      Map(
        "valueBytes" -> AttributeValue.builder.b(SdkBytes.fromByteArray(valueBytes)).build
      )
  }

  // Builds a DynamoDB query for a partition key with a time range on the sort key.
  def buildTimeRangeQuery(dataset: String,
                          partitionKeyBytes: Array[Byte],
                          startTs: Long,
                          endTs: Option[Long]): QueryRequest = {
    val partitionAlias = "#pk"
    val timeAlias = "#ts"
    val attrNameAliasMap = Map(partitionAlias -> partitionKeyColumn, timeAlias -> sortKeyColumn)
    val endTsResolved = endTs.getOrElse(System.currentTimeMillis())
    val attrValuesMap = Map(
      ":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build,
      ":start" -> AttributeValue.builder.n(startTs.toString).build,
      ":end" -> AttributeValue.builder.n(endTsResolved.toString).build
    )

    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue AND $timeAlias BETWEEN :start AND :end")
      .expressionAttributeNames(attrNameAliasMap.toJava)
      .expressionAttributeValues(attrValuesMap.toJava)
      .build
  }

  // Queries by partition key only — used for time-sorted tables when the sort key is unknown,
  // e.g. schema/metadata rows whose write timestamp isn't tracked at read time.
  def buildPartitionOnlyQuery(dataset: String, partitionKeyBytes: Array[Byte]): QueryRequest = {
    val partitionAlias = "#pk"
    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue")
      .expressionAttributeNames(Map(partitionAlias -> partitionKeyColumn).toJava)
      .expressionAttributeValues(
        Map(":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build).toJava)
      .build
  }
}
