package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.AvroConversions
import org.junit.Assert.{assertEquals, assertNotEquals, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

class JoinRequestKeysTest extends AnyFlatSpec {

  private val groupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.query_group_by"),
    keyColumns = Seq("query_normalized")
  )

  private val productGroupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.product_hydrate"),
    keyColumns = Seq("product_id")
  )

  private def join(selectExpr: String, setups: Seq[String] = Seq.empty): Join =
    Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.query_join"),
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("query_normalized" -> selectExpr),
          setups = setups
        ),
        table = "unit_test.queries"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = groupBy,
          keyMapping = Map("query_normalized" -> "query_normalized")
        ))
    )

  private def productViewJoin(selects: Map[String, String]): Join =
    Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.product_views_hydrated"),
      left = Builders.Source.events(
        query = Builders.Query(selects = selects),
        table = "unit_test.product_views"
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = productGroupBy,
          keyMapping = Map("product_id" -> "product_id")
        ))
    )

  private def servingInfo(inputSchema: StructType,
                          gb: GroupBy = groupBy,
                          keyFields: Array[StructField] = Array(StructField("query_normalized", StringType)))
      : GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(gb)
    groupByServingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(StructType("Key", keyFields)).toString)
    groupByServingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString)
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  private def productServingInfo(inputSchema: StructType): GroupByServingInfoParsed =
    servingInfo(
      inputSchema,
      gb = productGroupBy,
      keyFields = Array(StructField("product_id", LongType))
    )

  it should "include join left selects in cached key mapping keys" in {
    val firstJoin = join("lower(query)")
    val secondJoin = join("trim(lower(query))")

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include join left setups in part keys" in {
    val firstJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'first.Normalize'"))
    val secondJoin = join("normalize(query)", Seq("CREATE TEMPORARY FUNCTION normalize AS 'second.Normalize'"))

    assertNotEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "include GroupBy serving schema in schema-qualified part keys" in {
    val queryJoin = join("lower(query)")
    val firstServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))
    val secondServingInfo =
      servingInfo(StructType("Input", Array(StructField("query", StringType), StructField("locale", StringType))))

    assertNotEquals(
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, firstServingInfo),
      JoinRequestKeys.partKey(queryJoin, queryJoin.joinPartOps.head, secondServingInfo)
    )
  }

  it should "keep part keys stable for equivalent select ordering" in {
    def orderedJoin(selects: Map[String, String]): Join =
      Builders.Join(
        left = Builders.Source.events(
          query = Builders.Query(selects = selects),
          table = "unit_test.queries"
        ),
        joinParts = Seq(
          Builders.JoinPart(
            groupBy = groupBy,
            keyMapping = Map("query_normalized" -> "query_normalized")
          ))
      )

    val firstJoin = orderedJoin(Map("query_normalized" -> "lower(query)", "unused" -> "unused"))
    val secondJoin = orderedJoin(Map("unused" -> "unused", "query_normalized" -> "lower(query)"))

    assertEquals(
      JoinRequestKeys.partKey(firstJoin, firstJoin.joinPartOps.head),
      JoinRequestKeys.partKey(secondJoin, secondJoin.joinPartOps.head)
    )
  }

  it should "validate missing request keys against raw selected inputs" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head

    assertEquals(
      Seq("query"),
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map.empty), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(Request(queryJoin.metaData.name, Map("query" -> "SHOES")), queryJoin, joinPart)
    )
    assertEquals(
      Seq.empty,
      JoinRequestKeys.missingRequestKeys(
        Request(queryJoin.metaData.name, Map("query_normalized" -> "shoes")),
        queryJoin,
        joinPart)
    )
  }

  it should "derive GroupBy keys from raw request keys" in {
    val queryJoin = join("lower(query)")
    val joinPart = queryJoin.joinPartOps.head
    val request = Request(queryJoin.metaData.name, Map("query" -> "SHOES"))
    val keyServingInfo = servingInfo(StructType("Input", Array(StructField("query", StringType))))

    assertEquals(true, JoinRequestKeys.needsDerivation(request, queryJoin, joinPart))
    assertEquals(
      Map("query_normalized" -> "shoes"),
      JoinRequestKeys.deriveLeftKeys(request, queryJoin, joinPart, keyServingInfo)
    )
  }

  it should "infer nested struct types from left-key select paths" in {
    assertEquals(
      "data" -> StructType("productId_struct", Array(StructField("productId", LongType))),
      JoinRequestKeys.requestFieldType(Seq("data", "productId"), LongType)
    )
    assertEquals(
      "data" -> StructType(
        "baseEvent_struct",
        Array(
          StructField(
            "baseEvent",
            StructType("userId_struct", Array(StructField("userId", LongType)))
          ))),
      JoinRequestKeys.requestFieldType(Seq("data", "baseEvent", "userId"), LongType)
    )
  }

  it should "merge nested struct types that share a root" in {
    val productIdType = JoinRequestKeys.requestFieldType(Seq("data", "productId"), LongType)._2
    val userIdType = JoinRequestKeys.requestFieldType(Seq("data", "baseEvent", "userId"), LongType)._2
    val merged = JoinRequestKeys.mergeTypes(productIdType, userIdType).asInstanceOf[StructType]

    assertEquals(Set("productId", "baseEvent"), merged.fields.map(_.name).toSet)
    assertEquals(LongType, merged.typeOf("productId").get)
    val baseEvent = merged.typeOf("baseEvent").get.asInstanceOf[StructType]
    assertEquals(LongType, baseEvent.typeOf("userId").get)
  }

  it should "build join codec key mapping for nested Avro left selects without using right GroupBy input schema" in {
    // Right GroupBy input schema has no usable nested `data` struct (or a wrong scalar), which is
    // what previously caused rawInputType to fall back to the product_id LongType.
    val wrongHydrateInput = StructType(
      "HydrateInput",
      Array(
        StructField("product_id", LongType),
        StructField("data", StringType)
      )
    )
    val keyServingInfo = productServingInfo(wrongHydrateInput)
    val viewsJoin = productViewJoin(
      Map(
        "product_id" -> "CAST(data.productId AS BIGINT)",
        "user_id" -> "CAST(data.baseEvent.userId AS BIGINT)"
      ))
    val joinPart = viewsJoin.joinPartOps.head

    val keyMapping = JoinRequestKeys.buildKeyMapping(viewsJoin, joinPart, keyServingInfo)
    val dataField = keyMapping.requestKeyFields.find(_.name == "data").get
    assertTrue(dataField.fieldType.isInstanceOf[StructType])
    val dataStruct = dataField.fieldType.asInstanceOf[StructType]
    assertEquals(LongType, dataStruct.typeOf("productId").get)

    // Catalyst planning must succeed (this is where Flink join-codec build previously failed).
    assertTrue(keyMapping.catalystUtil.isDefined)
  }

  it should "type deeper nested left selects for join codec Catalyst planning" in {
    val keyServingInfo = productServingInfo(StructType("HydrateInput", Array(StructField("seller_id", LongType))))
    val viewsJoin = productViewJoin(
      Map(
        "product_id" -> "CAST(data.baseEvent.userId AS BIGINT)"
      ))
    val keyMapping = JoinRequestKeys.buildKeyMapping(viewsJoin, viewsJoin.joinPartOps.head, keyServingInfo)
    val dataStruct = keyMapping.requestKeyFields.find(_.name == "data").get.fieldType.asInstanceOf[StructType]
    val baseEvent = dataStruct.typeOf("baseEvent").get.asInstanceOf[StructType]
    assertEquals(LongType, baseEvent.typeOf("userId").get)
    assertTrue(keyMapping.catalystUtil.isDefined)
  }
}
