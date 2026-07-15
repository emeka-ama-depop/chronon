package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.Request
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import scala.collection.mutable

/** Translates a Join request's keys into the keys needed by each underlying GroupBy fetch.
  *
  * GroupBys are fetched by their source keys, but a Join can define left-side selects that derive those keys from the
  * raw request. For example, a Join may receive `query`, select `query_normalized = lower(query)`, and then map
  * `query_normalized` to a GroupBy key. In that case the online Join fetch path should accept `query` from callers,
  * derive `query_normalized` inside the fetcher, and use the derived value only when building the GroupBy request.
  *
  * The helper keeps that flow local to Join fetching:
  *   - `buildKeyMapping` is run while building the JoinCodec so SQL parsing, input-schema construction, and Catalyst
  *     wrapper setup are cached with the Join metadata instead of repeated per request.
  *   - `requestKeyFields` reports the raw request keys needed to derive selected Join keys, plus the derived key aliases
  *     themselves so existing direct-key callers can still be logged against the Join key schema.
  *   - `valueInfoLeftKeys` reports only the raw request keys for selected Join keys, so fetchJoinSchema does not imply
  *     that callers must provide derived keys.
  *   - `missingRequestKeys` validates against those raw inputs, while still accepting a derived key if the caller
  *     provides it directly.
  *   - `KeyMapping.leftKeys` runs the cached Join left-select transform when needed, then combines derived and directly
  *     supplied left keys before JoinPartFetcher maps them to right-side GroupBy keys.
  */
private[online] object JoinRequestKeys {
  case class KeyMapping(leftToRight: Map[String, String],
                        requestKeyFields: Iterable[StructField],
                        rawInputsByLeftKey: Map[String, Seq[String]],
                        selectedLeftKeys: Seq[(String, String)],
                        catalystUtil: Option[PooledCatalystUtil]) {
    private val selectedExpressions = selectedLeftKeys.toMap

    private def shouldDeriveLeftKey(request: Request, leftKey: String): Boolean =
      selectedExpressions.contains(leftKey) &&
        (!request.keys.contains(leftKey) || rawInputsByLeftKey.getOrElse(leftKey, Seq.empty).contains(leftKey))

    def missingRequestKeys(request: Request): Seq[String] =
      leftToRight.keys.toSeq.flatMap { leftKey =>
        if (request.keys.contains(leftKey)) {
          Seq.empty
        } else {
          rawInputsByLeftKey
            .get(leftKey)
            .filter(_ => selectedExpressions.contains(leftKey))
            .getOrElse(Seq(leftKey))
            .filterNot(request.keys.contains)
        }
      }.distinct

    def leftKeys(request: Request): Map[String, AnyRef] = {
      val directLeftKeys = leftToRight.keys.collect {
        case leftKey if request.keys.contains(leftKey) && !shouldDeriveLeftKey(request, leftKey) =>
          leftKey -> request.keys(leftKey)
      }.toMap

      val keysToDerive = selectedLeftKeys.filter { case (leftKey, _) => shouldDeriveLeftKey(request, leftKey) }
      val derivedLeftKeys =
        if (keysToDerive.isEmpty) {
          Map.empty[String, Any]
        } else {
          val derivedValues = catalystUtil
            .map(_.performSql(request.keys).headOption.getOrElse(Map.empty))
            .getOrElse(Map.empty)
          keysToDerive.map { case (leftKey, _) =>
            leftKey -> derivedValues.getOrElse(leftKey, null)
          }.toMap
        }

      (directLeftKeys ++ derivedLeftKeys).map { case (key, value) => key -> value.asInstanceOf[AnyRef] }
    }
  }

  private def partKey(parts: Seq[String]): String =
    parts.map(part => s"${part.length}:$part").mkString("|")

  private def partKeyParts(join: Join, joinPart: JoinPartOps): Seq[String] =
    Seq(
      joinPart.groupBy.metaData.getName,
      Option(joinPart.prefix).getOrElse("")
    ) ++
      joinPart.leftToRight.toSeq.sortBy(_._1).map { case (left, right) => s"key:$left=$right" } ++
      leftSelects(join).toSeq.sortBy(_._1).map { case (name, expr) => s"select:$name=$expr" } ++
      leftSetups(join).map(setup => s"setup:$setup")

  def partKey(join: Join, joinPart: JoinPartOps): String =
    partKey(partKeyParts(join, joinPart))

  def partKey(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): String =
    partKey(partKeyParts(join, joinPart) ++ Seq(servingInfo.keyAvroSchema, servingInfo.inputAvroSchema))

  private def leftSelects(join: Join): Map[String, String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .flatMap(query => Option(query.getQuerySelects))
      .getOrElse(Map.empty)

  private def leftSetups(join: Join): Seq[String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .map(_.setupsSeq)
      .getOrElse(Seq.empty)

  private def selectExpression(join: Join, leftKey: String): Option[String] =
    leftSelects(join).get(leftKey).filter(_ != leftKey)

  private[fetcher] def rawInputs(expression: String): Seq[String] =
    rawAttributePaths(expression).map(_.head).distinct

  /** Full attribute paths referenced by a select expression, e.g. data.productId -> Seq("data", "productId"). */
  private[fetcher] def rawAttributePaths(expression: String): Seq[Seq[String]] =
    CatalystSqlParser
      .parseExpression(expression)
      .collect { case attr: UnresolvedAttribute =>
        attr.nameParts.toSeq
      }
      .filter(_.nonEmpty)
      .distinct

  /** Build the Catalyst input type for a top-level request field from a nested attribute path.
    *
    * Left-key selects often extract flat Join keys from nested Kafka envelopes
    * (CAST(data.productId AS BIGINT)). Join metadata does not carry the left topic schema at codec
    * build time, and the right GroupBy input schema is a different entity — so we infer a minimal
    * struct shaped like the path, with the mapped GroupBy key type as the leaf.
    */
  private[fetcher] def requestFieldType(path: Seq[String], leafType: DataType): (String, DataType) = {
    require(path.nonEmpty, "attribute path must be non-empty")
    val nestedType = path.tail.foldRight(leafType) { (name, innerType) =>
      StructType(s"${name}_struct", Array(StructField(name, innerType)))
    }
    path.head -> nestedType
  }

  private[fetcher] def mergeTypes(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (leftStruct: StructType, rightStruct: StructType) =>
        val merged = mutable.LinkedHashMap.empty[String, DataType]
        (leftStruct.fields ++ rightStruct.fields).foreach { field =>
          merged.get(field.name) match {
            case None             => merged.put(field.name, field.fieldType)
            case Some(existing)   => merged.put(field.name, mergeTypes(existing, field.fieldType))
          }
        }
        StructType(leftStruct.name, merged.map { case (name, dataType) => StructField(name, dataType) }.toArray)
      case (leftStruct: StructType, _) => leftStruct
      case (_, rightStruct: StructType) => rightStruct
      case _ if left == right           => left
      case _                            => left
    }

  def valueInfoLeftKeys(join: Join, joinPart: JoinPartOps): Iterable[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey))
    }.distinct

  private def requestKeyFields(join: Join,
                               joinPart: JoinPartOps,
                               servingInfo: GroupByServingInfoParsed): Iterable[StructField] = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val fieldsByRightKey = keySchema.fields.map(field => field.name -> field).toMap
    val fieldsByRequestKey = mutable.LinkedHashMap.empty[String, DataType]

    def putOrMerge(name: String, dataType: DataType): Unit =
      fieldsByRequestKey.get(name) match {
        case None           => fieldsByRequestKey.put(name, dataType)
        case Some(existing) => fieldsByRequestKey.put(name, mergeTypes(existing, dataType))
      }

    joinPart.leftToRight.foreach { case (leftKey, rightKey) =>
      val leafType = fieldsByRightKey
        .getOrElse(
          rightKey,
          throw new IllegalArgumentException(
            s"Join part ${joinPart.fullPrefix} maps left key $leftKey to right key $rightKey, " +
              s"but $rightKey is not present in GroupBy key schema ${keySchema.fields.map(_.name).mkString(", ")}")
        )
        .fieldType

      selectExpression(join, leftKey) match {
        case Some(expression) =>
          rawAttributePaths(expression).foreach { path =>
            val (root, dataType) = requestFieldType(path, leafType)
            putOrMerge(root, dataType)
          }
        case None =>
          putOrMerge(leftKey, leafType)
      }

      if (!fieldsByRequestKey.contains(leftKey)) {
        putOrMerge(leftKey, leafType)
      }
    }

    fieldsByRequestKey.map { case (name, dataType) => StructField(name, dataType) }
  }

  def buildKeyMapping(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): KeyMapping = {
    val selectedLeftKeys = joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(leftKey -> _)
    }
    val rawInputsByLeftKey = joinPart.leftToRight.keys.map { leftKey =>
      leftKey -> selectedLeftKeys
        .find(_._1 == leftKey)
        .map { case (_, expression) =>
          rawInputs(expression)
        }
        .getOrElse(Seq(leftKey))
    }.toMap
    val keyFields = requestKeyFields(join, joinPart, servingInfo)
    val catalystUtil =
      if (selectedLeftKeys.isEmpty) None
      else
        Some(new PooledCatalystUtil(selectedLeftKeys, StructType("JoinRequest", keyFields.toArray), leftSetups(join)))

    KeyMapping(joinPart.leftToRight, keyFields, rawInputsByLeftKey, selectedLeftKeys, catalystUtil)
  }

  def missingRequestKeys(request: Request, join: Join, joinPart: JoinPartOps): Seq[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      if (request.keys.contains(leftKey)) {
        Seq.empty
      } else {
        selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey)).filterNot(request.keys.contains)
      }
    }.distinct

  private def shouldDeriveLeftKey(request: Request, join: Join, leftKey: String): Boolean =
    selectExpression(join, leftKey).exists { expression =>
      !request.keys.contains(leftKey) || rawInputs(expression).contains(leftKey)
    }

  def needsDerivation(request: Request, join: Join, joinPart: JoinPartOps): Boolean =
    joinPart.leftToRight.keys.exists(shouldDeriveLeftKey(request, join, _))

  def deriveLeftKeys(request: Request,
                     join: Join,
                     joinPart: JoinPartOps,
                     servingInfo: GroupByServingInfoParsed): Map[String, AnyRef] = {
    buildKeyMapping(join, joinPart, servingInfo).leftKeys(request)
  }
}
