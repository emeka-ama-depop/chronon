package ai.chronon.integrations.aws

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/** Wraps a DynamoDbAsyncClient to automatically prefix all table names.
  *
  * @param delegate the underlying DynamoDbAsyncClient to wrap
  * @param tablePrefix the prefix to apply to all table names
  */
class PrefixedDynamoDbAsyncClient(delegate: DynamoDbAsyncClient,
                                  tablePrefix: String = "",
                                  dataDelegateForTableName: String => Option[DynamoDbAsyncClient] = _ => None) {

  private val logger = LoggerFactory.getLogger(getClass)

  private def prefixTableName(tableName: String): String = {
    if (tableName == null || tableName.isEmpty) tableName
    else tablePrefix + tableName
  }

  private def isTablePrefixed(tableName: String): Boolean = {
    tablePrefix.nonEmpty && tableName.startsWith(tablePrefix)
  }

  private def dataDelegateFor(tableName: String): DynamoDbAsyncClient =
    dataDelegateForTableName(tableName).getOrElse(delegate)

  // ========== Supported Read Operations ==========

  def getItem(request: GetItemRequest): CompletableFuture[GetItemResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"getItem: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    dataDelegateFor(originalTableName).getItem(prefixedRequest)
  }

  def query(request: QueryRequest): CompletableFuture[QueryResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"query: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.query(prefixedRequest)
  }

  /** BatchGetItem can span tables, but Chronon's item-cache reads are deliberately single-table so one
    * table-specific DAX delegate owns the whole request. Response table names are normalized back to the
    * unprefixed name for the KV store.
    */
  def batchGetItem(request: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
    val requestItems = request.requestItems().asScala
    require(requestItems.size == 1, "PrefixedDynamoDbAsyncClient only supports single-table BatchGetItem requests")

    val (originalTableName, keysAndAttributes) = requestItems.head
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"batchGetItem: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder
      .requestItems(Map(prefixedTableName -> keysAndAttributes).asJava)
      .build()

    dataDelegateFor(originalTableName).batchGetItem(prefixedRequest).thenApply { response =>
      val normalizedResponses = Option(response.responses())
        .flatMap(responses => Option(responses.get(prefixedTableName)))
        .map(items => Map(originalTableName -> items).asJava)
        .getOrElse(Map.empty[String, java.util.List[java.util.Map[String, AttributeValue]]].asJava)
      val normalizedUnprocessedKeys = Option(response.unprocessedKeys())
        .flatMap(unprocessed => Option(unprocessed.get(prefixedTableName)))
        .map(keys => Map(originalTableName -> keys).asJava)
        .getOrElse(Map.empty[String, KeysAndAttributes].asJava)

      response.toBuilder
        .responses(normalizedResponses)
        .unprocessedKeys(normalizedUnprocessedKeys)
        .build()
    }
  }

  def scan(request: ScanRequest): CompletableFuture[ScanResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"scan: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.scan(prefixedRequest)
  }

  def describeTable(request: DescribeTableRequest): CompletableFuture[DescribeTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"describeTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.describeTable(prefixedRequest)
  }

  def describeImport(request: DescribeImportRequest): CompletableFuture[DescribeImportResponse] = {
    // DescribeImport uses ARN, not table name, so no prefixing needed
    delegate.describeImport(request)
  }

  // ========== Supported Write Operations ==========

  def putItem(request: PutItemRequest): CompletableFuture[PutItemResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"putItem: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    dataDelegateFor(originalTableName).putItem(prefixedRequest)
  }

  def createTable(request: CreateTableRequest): CompletableFuture[CreateTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"createTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.createTable(prefixedRequest)
  }

  /** Lists tables whose names start with the given prefix (after applying the client table prefix).
    * The response table names have the client prefix stripped so callers see logical names.
    */
  def listTables(request: ListTablesRequest): CompletableFuture[ListTablesResponse] = {
    // Prefix the exclusiveStartTableName if present
    val prefixedRequest = if (request.exclusiveStartTableName() != null) {
      request.toBuilder.exclusiveStartTableName(prefixTableName(request.exclusiveStartTableName())).build()
    } else {
      request
    }
    delegate.listTables(prefixedRequest).thenApply { response =>
      // Strip the client prefix from returned table names so callers see logical names
      val strippedNames = response.tableNames().toArray(new Array[String](0)).map { name =>
        if (isTablePrefixed(name)) name.substring(tablePrefix.length)
        else name
      }
      val builder = response.toBuilder.tableNames(strippedNames: _*)
      // Strip prefix from lastEvaluatedTableName if present
      val lastEval = response.lastEvaluatedTableName()
      if (lastEval != null && tablePrefix.nonEmpty && lastEval.startsWith(tablePrefix)) {
        builder.lastEvaluatedTableName(lastEval.substring(tablePrefix.length)).build()
      } else {
        builder.build()
      }
    }
  }

  def deleteTable(request: DeleteTableRequest): CompletableFuture[DeleteTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"deleteTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.deleteTable(prefixedRequest)
  }

  def updateTimeToLive(request: UpdateTimeToLiveRequest): CompletableFuture[UpdateTimeToLiveResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(
      s"updateTimeToLive: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.updateTimeToLive(prefixedRequest)
  }

  def updateTable(request: UpdateTableRequest): CompletableFuture[UpdateTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"updateTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.updateTable(prefixedRequest)
  }

  def importTable(request: ImportTableRequest): CompletableFuture[ImportTableResponse] = {
    // For ImportTableRequest, the table name is in tableCreationParameters
    val originalParams = request.tableCreationParameters()
    if (originalParams != null && originalParams.tableName() != null) {
      val originalTableName = originalParams.tableName()
      val prefixedTableName = prefixTableName(originalTableName)
      logger.debug(s"importTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
      val prefixedParams = originalParams.toBuilder
        .tableName(prefixedTableName)
        .build()
      val prefixedRequest = request.toBuilder
        .tableCreationParameters(prefixedParams)
        .build()
      delegate.importTable(prefixedRequest)
    } else {
      delegate.importTable(request)
    }
  }

  // ========== Waiters ==========

  def waitUntilTableExists(tableName: String)
      : CompletableFuture[software.amazon.awssdk.core.waiters.WaiterResponse[DescribeTableResponse]] = {
    val prefixedTableName = prefixTableName(tableName)
    logger.debug(s"waitUntilTableExists: original table name='$tableName' -> prefixed table name='$prefixedTableName'")
    val request = DescribeTableRequest.builder.tableName(prefixedTableName).build
    delegate.waiter().waitUntilTableExists(request)
  }

  def waitUntilTableNotExists(tableName: String)
      : CompletableFuture[software.amazon.awssdk.core.waiters.WaiterResponse[DescribeTableResponse]] = {
    val prefixedTableName = prefixTableName(tableName)
    logger.debug(
      s"waitUntilTableNotExists: original table name='$tableName' -> prefixed table name='$prefixedTableName'")
    val request = DescribeTableRequest.builder.tableName(prefixedTableName).build
    delegate.waiter().waitUntilTableNotExists(request)
  }
}
