package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{KvDaxEndpointArg, KvEnableDaxArg}
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class DynamoDBBatchTableRegistryTest extends AnyFlatSpec with BeforeAndAfterAll {

  import DynamoDBKVStoreConstants._

  var dynamoContainer: GenericContainer[_] = _
  var client: DynamoDbAsyncClient = _

  override def beforeAll(): Unit = {
    dynamoContainer = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest"))
    dynamoContainer.withExposedPorts(8000: Integer)
    dynamoContainer.withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
    dynamoContainer.start()

    val dynamoEndpoint = s"http://${dynamoContainer.getHost}:${dynamoContainer.getMappedPort(8000)}"
    client = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create(dynamoEndpoint))
      .region(Region.US_WEST_2)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
        ))
      .build()
  }

  override def afterAll(): Unit = {
    if (client != null) client.close()
    if (dynamoContainer != null) dynamoContainer.stop()
  }

  it should "resolve batch dataset to physical table via registry" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    kvStore.create(batchTableRegistry)

    val logicalName = "MY_GROUPBY_BATCH"
    val physicalName = "MY_GROUPBY_BATCH_2026_02_17"

    Await.result(
      kvStore.multiPut(Seq(PutRequest(logicalName.getBytes(StandardCharsets.UTF_8), physicalName.getBytes(StandardCharsets.UTF_8), batchTableRegistry))),
      1.minute
    )

    val resolved = kvStore.resolveTableName(logicalName)
    resolved shouldBe physicalName
  }

  it should "publish DAX endpoint in batch table registry values" in {
    val defaultStore = new DynamoDBKVStoreImpl(client)
    val daxStore = new DynamoDBKVStoreImpl(
      client,
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> "dax://test-cluster.example.com")
    )

    defaultStore.batchTableRegistryValue("DEFAULT_BATCH_2026_02_17") shouldBe "DEFAULT_BATCH_2026_02_17"
    daxStore.batchTableRegistryValue("DAX_BATCH_2026_02_17") shouldBe
      "dax://test-cluster.example.com@DAX_BATCH_2026_02_17"
    batchTableInfoFromRegistryValue(daxStore.batchTableRegistryValue("DAX_BATCH_2026_02_17")) shouldBe
      BatchTableInfo("DAX_BATCH_2026_02_17", Some("dax://test-cluster.example.com"))
  }

  it should "use dataset name directly for non-batch datasets" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    val dataset = "MY_GROUPBY_STREAMING"
    val resolved = kvStore.resolveTableName(dataset)
    resolved shouldBe dataset
  }

  it should "return cached value on subsequent lookups" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    kvStore.create(batchTableRegistry)

    val logicalName = "CACHED_GROUPBY_BATCH"
    val physicalName = "CACHED_GROUPBY_BATCH_2026_02_17"

    Await.result(
      kvStore.multiPut(Seq(PutRequest(logicalName.getBytes(StandardCharsets.UTF_8), physicalName.getBytes(StandardCharsets.UTF_8), batchTableRegistry))),
      1.minute
    )

    // First lookup populates the cache
    val resolved1 = kvStore.resolveTableName(logicalName)
    resolved1 shouldBe physicalName

    // Update the registry to a different value
    Await.result(
      kvStore.multiPut(Seq(PutRequest(logicalName.getBytes(StandardCharsets.UTF_8), "CACHED_GROUPBY_BATCH_2026_02_18".getBytes(StandardCharsets.UTF_8), batchTableRegistry))),
      1.minute
    )

    // Second lookup should still return cached value (TTL hasn't expired)
    val resolved2 = kvStore.resolveTableName(logicalName)
    resolved2 shouldBe physicalName
  }

  it should "resolve batch table names in multiGet for get lookups" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    kvStore.create(batchTableRegistry)

    val logicalName = "MULTIGET_GROUPBY_BATCH"
    val physicalName = "MULTIGET_GROUPBY_BATCH_2026_02_17"

    // Write registry entry
    Await.result(
      kvStore.multiPut(Seq(PutRequest(logicalName.getBytes(StandardCharsets.UTF_8), physicalName.getBytes(StandardCharsets.UTF_8), batchTableRegistry))),
      1.minute
    )

    // Create the physical table and insert data
    kvStore.create(physicalName, Map.empty)
    val keyBytes = "test-key".getBytes(StandardCharsets.UTF_8)
    val valueBytes = "test-value".getBytes(StandardCharsets.UTF_8)
    val putResult = Await.result(kvStore.multiPut(Seq(PutRequest(keyBytes, valueBytes, physicalName, None))), 1.minute)
    putResult shouldBe Seq(true)

    // multiGet using the logical name should resolve and read from the physical table
    val getRequest = GetRequest(keyBytes, logicalName, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResult.length shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    val returnedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    returnedValue shouldBe "test-value"
  }

  it should "use dataset name directly in multiGet for non-batch datasets" in {
    val kvStore = new DynamoDBKVStoreImpl(client)

    val dataset = "DIRECT_TABLE"
    kvStore.create(dataset, Map.empty)

    val keyBytes = "direct-key".getBytes(StandardCharsets.UTF_8)
    val valueBytes = "direct-value".getBytes(StandardCharsets.UTF_8)
    val putResult = Await.result(kvStore.multiPut(Seq(PutRequest(keyBytes, valueBytes, dataset, None))), 1.minute)
    putResult shouldBe Seq(true)

    val getRequest = GetRequest(keyBytes, dataset, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 1.minute)
    getResult.length shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    val returnedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    returnedValue shouldBe "direct-value"
  }

}
