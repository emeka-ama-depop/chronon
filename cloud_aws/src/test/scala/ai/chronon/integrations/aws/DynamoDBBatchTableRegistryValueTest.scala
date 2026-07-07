package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{KvDaxEndpointArg, KvEnableDaxArg, MetadataDataset}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class DynamoDBBatchTableRegistryValueTest extends AnyFlatSpec with Matchers {

  import DynamoDBKVStoreConstants._

  "DynamoDB batch table registry values" should "read physical table names" in {
    val store = new DynamoDBKVStoreImpl(null.asInstanceOf[DynamoDbAsyncClient])

    store.batchTableRegistryValue("MY_GROUPBY_BATCH_2026_02_17") shouldBe "MY_GROUPBY_BATCH_2026_02_17"
    batchTableInfoFromRegistryValue("MY_GROUPBY_BATCH_2026_02_17") shouldBe
      BatchTableInfo("MY_GROUPBY_BATCH_2026_02_17", daxEndpoint = None)
  }

  it should "write physical table names without DAX metadata" in {
    val registryValue = registryValueForBatchTable("MY_GROUPBY_BATCH_2026_02_17")

    registryValue shouldBe "MY_GROUPBY_BATCH_2026_02_17"
  }

  it should "encode DAX endpoint with physical table names when DAX is configured" in {
    val registryValue =
      registryValueForBatchTable("MY_GROUPBY_BATCH_2026_02_17", Some("dax://test-cluster.example.com"))

    registryValue shouldBe "dax://test-cluster.example.com@MY_GROUPBY_BATCH_2026_02_17"
    batchTableInfoFromRegistryValue(registryValue) shouldBe
      BatchTableInfo("MY_GROUPBY_BATCH_2026_02_17", Some("dax://test-cluster.example.com"))
  }

  it should "publish DAX endpoint only when DAX is configured" in {
    val defaultStore = new DynamoDBKVStoreImpl(null.asInstanceOf[DynamoDbAsyncClient])
    val missingEndpointStore =
      new DynamoDBKVStoreImpl(null.asInstanceOf[DynamoDbAsyncClient], Map(KvEnableDaxArg -> "true"))
    val daxStore = new DynamoDBKVStoreImpl(
      null.asInstanceOf[DynamoDbAsyncClient],
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> "dax://test-cluster.example.com")
    )

    defaultStore.batchTableRegistryValue("DEFAULT_BATCH_2026_02_17") shouldBe "DEFAULT_BATCH_2026_02_17"
    missingEndpointStore.batchTableRegistryValue("MISSING_ENDPOINT_BATCH_2026_02_17") shouldBe
      "MISSING_ENDPOINT_BATCH_2026_02_17"
    daxStore.batchTableRegistryValue("DAX_BATCH_2026_02_17") shouldBe
      "dax://test-cluster.example.com@DAX_BATCH_2026_02_17"
  }

  it should "only enable DAX for uploads when config enables DAX and endpoint is configured" in {
    val disabledStore = new DynamoDBKVStoreImpl(null.asInstanceOf[DynamoDbAsyncClient])
    val missingEndpointStore = new DynamoDBKVStoreImpl(
      null.asInstanceOf[DynamoDbAsyncClient],
      Map(KvEnableDaxArg -> "true")
    )
    val enabledStore = new DynamoDBKVStoreImpl(
      null.asInstanceOf[DynamoDbAsyncClient],
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> "dax://test-cluster.example.com")
    )

    disabledStore.daxEnabled shouldBe false
    missingEndpointStore.daxEnabled shouldBe false
    enabledStore.daxEnabled shouldBe true
  }

  it should "not use DAX for metadata datasets" in {
    val enabledStore = new DynamoDBKVStoreImpl(
      null.asInstanceOf[DynamoDbAsyncClient],
      Map(KvEnableDaxArg -> "true", KvDaxEndpointArg -> "dax://test-cluster.example.com")
    )

    enabledStore.isCacheEligible("FEATURE_STREAMING") shouldBe true
    enabledStore.isCacheEligible(MetadataDataset) shouldBe false
    enabledStore.isCacheEligible(batchTableRegistry) shouldBe false
  }

  it should "pass the configured AWS region to DAX configuration" in {
    val expectedRegion = sys.env.getOrElse("AWS_DEFAULT_REGION", "us-west-2")
    val store = new DynamoDBKVStoreImpl(
      null.asInstanceOf[DynamoDbAsyncClient],
      Map("AWS_DEFAULT_REGION" -> expectedRegion, KvDaxEndpointArg -> "dax://test-cluster.example.com")
    )

    val testCredentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    store.daxConfiguration("dax://test-cluster.example.com", Some(testCredentials)).region() shouldBe Region.of(expectedRegion)
  }
}
