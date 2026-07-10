package ai.chronon.spark.batch.iceberg

import ai.chronon.api.ScalaJavaConversions.JMapOps
import ai.chronon.api.{PartitionRange, PartitionSpec, ThriftJsonCodec}
import ai.chronon.observability._
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.spark.catalog.{Format, Iceberg}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.iceberg.DataFile
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Type
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable

object IcebergPartitionStatsExtractor {
  type PartitionKey = Iceberg.PartitionKey

  case class IcebergPartitionStatsResult(
      tileSummaries: Map[TileSummaryKey, TileSummary],
      partitionRowCounts: Map[PartitionKey, Long]
  )

  private[iceberg] def partitionKeyInRange(partitionKey: PartitionKey, requestedRange: Option[PartitionRange])(implicit
      partitionSpec: PartitionSpec): Boolean =
    requestedRange.forall { range =>
      val startMillis = Option(range.start)
        .map(range.partitionSpec.epochMillis)
        .getOrElse(Long.MinValue)
      val endExclusiveMillis = Option(range.end)
        .map(end => range.partitionSpec.epochMillis(end) + range.partitionSpec.spanMillis)
        .getOrElse(Long.MaxValue)

      partitionKey.toMap.get(partitionSpec.column).exists { value =>
        val partitionMillis = partitionSpec.epochMillis(value)
        partitionMillis >= startMillis && partitionMillis < endExclusiveMillis
      }
    }

  private[iceberg] def rangeFilterExpression(schema: org.apache.iceberg.Schema, range: Option[PartitionRange])(implicit
      partitionSpec: PartitionSpec): Option[Expression] =
    for {
      requestedRange <- range
      field <- Option(schema.findField(partitionSpec.column))
      expression <- buildRangeExpression(requestedRange, field.`type`())
    } yield expression

  private def buildRangeExpression(range: PartitionRange, fieldType: Type)(implicit
      partitionSpec: PartitionSpec): Option[Expression] = {
    // Normalize through millis so mixed PartitionSpec formats still produce one half-open typed Iceberg predicate.
    val lower = Option(range.start).flatMap { start =>
      literalForMillis(range.partitionSpec.epochMillis(start), fieldType)
    }
    val upper = Option(range.end).flatMap { end =>
      val endExclusiveMillis = range.partitionSpec.epochMillis(end) + range.partitionSpec.spanMillis
      literalForMillis(endExclusiveMillis, fieldType)
    }

    val predicates = Seq(
      lower.map(value => Expressions.greaterThanOrEqual[AnyRef](partitionSpec.column, value)),
      upper.map(value => Expressions.lessThan[AnyRef](partitionSpec.column, value))
    ).flatten

    predicates.reduceOption(Expressions.and)
  }

  private def literalForMillis(millis: Long, fieldType: Type)(implicit partitionSpec: PartitionSpec): Option[AnyRef] =
    fieldType.typeId() match {
      case Type.TypeID.STRING =>
        Some(partitionSpec.at(millis))
      case Type.TypeID.DATE =>
        // Iceberg DateLiteral uses days since epoch, matching DATE file-stat bounds.
        Some(Integer.valueOf(Math.toIntExact(Math.floorDiv(millis, 24L * 60 * 60 * 1000))))
      case Type.TypeID.TIMESTAMP =>
        // Iceberg TimestampLiteral uses micros since epoch, matching TIMESTAMP file-stat bounds.
        Some(java.lang.Long.valueOf(Math.multiplyExact(millis, 1000L)))
      case _ =>
        None
    }

  def extractPartitionMillisFromSlice(slice: String, partitionSpec: PartitionSpec): Long = {
    val partitionPairs =
      try {
        Iceberg.parsePartitionPath(slice).toMap
      } catch {
        case e: IllegalStateException => throw new IllegalArgumentException(e.getMessage, e)
      }

    // Extract the value for the partition column used by PartitionSpec
    val partitionValue = partitionPairs.getOrElse(
      partitionSpec.column,
      throw new IllegalArgumentException(s"Partition column '${partitionSpec.column}' not found in slice: $slice"))

    // Convert partition value to millis using PartitionSpec
    partitionSpec.epochMillis(partitionValue)
  }

  def createPartitionStatsPutRequest(
      outputTable: String,
      partitionStats: TileStats,
      dayPartitionMillis: Long,
      statsType: TileStatsType
  ): PutRequest = {
    val key = createMetricsRowKey(outputTable, statsType)
    val value = ThriftJsonCodec.toCompactBase64(partitionStats).getBytes
    val datasetName = s"${outputTable}_BATCH"
    PutRequest(key, value, datasetName, Some(dayPartitionMillis))
  }

  def createNullCountsStats(
      columnTileSummaries: Iterable[(TileSummaryKey, TileSummary)]
  ): NullCounts = {
    val nullCounts = columnTileSummaries.map { case (tileKey, tileSummary) =>
      val fieldId = tileKey.getColumn.toInt
      fieldId -> (if (tileSummary.isSetNullCount) tileSummary.getNullCount else 0L)
    }.toMap

    val rowCount = columnTileSummaries.headOption.map(_._2.getCount).getOrElse(0L)

    new NullCounts()
      .setRowCount(rowCount)
      .setNullCounts(nullCounts.map { case (k, v) =>
        k.asInstanceOf[java.lang.Integer] -> v.asInstanceOf[java.lang.Long]
      }.toJava)
  }

  def createMetricsRowKey(outputTable: String, statsType: TileStatsType): Array[Byte] = {
    s"${outputTable}#${statsType.name()}".getBytes(StandardCharsets.UTF_8)
  }

  def createSchemaMappingPutRequest(outputTable: String, fieldIdToNameMap: Map[Int, String]): PutRequest = {

    // Serialize the mapping to JSON
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val jsonValue = objectMapper.writeValueAsString(fieldIdToNameMap)

    // Create the row key following the same pattern as createMetricsRowKey
    val key = s"${outputTable}#schema".getBytes(StandardCharsets.UTF_8)
    val value = jsonValue.getBytes(StandardCharsets.UTF_8)
    val datasetName = s"${outputTable}_BATCH"

    PutRequest(key, value, datasetName)
  }

}

case class ColumnStats(
    nullCount: Long,
    distinctCount: Option[Long] = None,
    minValue: Option[Any] = None,
    maxValue: Option[Any] = None
) {
  def aggregate(other: ColumnStats): ColumnStats = {
    ColumnStats(
      nullCount = this.nullCount + other.nullCount,
      distinctCount = (this.distinctCount, other.distinctCount) match {
        case (Some(a), Some(b)) => Some(math.max(a, b)) // Conservative estimate - take max
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      minValue = (this.minValue, other.minValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = true))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      maxValue = (this.maxValue, other.maxValue) match {
        case (Some(a), Some(b)) => Some(compareValues(a, b, takeMin = false))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      }
    )
  }

  private def compareValues(a: Any, b: Any, takeMin: Boolean): Any = {
    try {
      (a, b) match {
        case (null, _) => if (takeMin) a else b
        case (_, null) => if (takeMin) a else b
        case (a1: java.lang.Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[java.lang.Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case (a1: Comparable[_], b1) if a1.getClass == b1.getClass =>
          val comparison = a1.asInstanceOf[Comparable[Any]].compareTo(b1)
          if ((comparison < 0) == takeMin) a1 else b1
        case _ => if (takeMin) a else b
      }
    } catch {
      case _: Exception => if (takeMin) a else b // Fallback to first value on comparison error
    }
  }
}

class PartitionAccumulator(
    val partitionKey: IcebergPartitionStatsExtractor.PartitionKey,
    val confName: String,
    val schema: org.apache.iceberg.Schema
)(implicit val partitionSpec: PartitionSpec) {
  var totalRowCount: Long = 0L
  val columnStats = mutable.Map[Int, ColumnStats]()

  def addFileStats(rowCount: Long, fileColumnStats: Map[Int, ColumnStats]): Unit = {
    totalRowCount += rowCount

    fileColumnStats.foreach { case (fieldId, stats) =>
      val currentStats = columnStats.getOrElse(fieldId, ColumnStats(0L))
      columnStats(fieldId) = currentStats.aggregate(stats)
    }
  }

  def toTileSummaries: Map[TileSummaryKey, TileSummary] = {
    columnStats.map { case (fieldId, stats) =>
      // Create partition key string from partition values
      val partitionKeyStr = partitionKey.map { case (col, value) => s"$col=$value" }.mkString("/")

      val tileKey = new TileSummaryKey()
        .setColumn(fieldId.toString)
        .setName(confName)
        .setSlice(partitionKeyStr) // Use slice to store partition key
        .setSizeMillis(partitionSpec.spanMillis)

      val tileSummary = new TileSummary()
        .setCount(totalRowCount)
        .setNullCount(stats.nullCount)

      tileKey -> tileSummary
    }.toMap
  }
}

class IcebergPartitionStatsExtractor(spark: SparkSession) {
  import IcebergPartitionStatsExtractor.{
    IcebergPartitionStatsResult,
    PartitionKey,
    partitionKeyInRange,
    rangeFilterExpression
  }

  private def loadIcebergTable(fullTableName: String): Option[org.apache.iceberg.Table] = {
    try {
      implicit val sparkSession: SparkSession = spark
      val resolved = Format.resolveTableName(fullTableName)
      val catalog = spark.sessionState.catalogManager
        .catalog(resolved.catalog)
        .asInstanceOf[TableCatalog]

      Option(catalog.loadTable(resolved.toIdentifier)) match {
        case Some(sparkTable: SparkTable) => Some(sparkTable.table())
        case _                            => None
      }
    } catch {
      case _: Exception => None
    }
  }

  def extractSchemaMapping(fullTableName: String): Option[Map[Int, String]] = {
    loadIcebergTable(fullTableName).flatMap { table =>
      Option(table.schema()).map { schema =>
        schema.columns().asScala.map(field => field.fieldId() -> field.name()).toMap
      }
    }
  }

  def extractPartitionedStats(fullTableName: String, confName: String, range: Option[PartitionRange] = None)(implicit
      partitionSpec: PartitionSpec): Option[Map[TileSummaryKey, TileSummary]] = {
    extractPartitionStatsWithRowCounts(fullTableName, confName, range).map(_.tileSummaries)
  }

  def extractPartitionStatsWithRowCounts(fullTableName: String, confName: String, range: Option[PartitionRange] = None)(
      implicit partitionSpec: PartitionSpec): Option[IcebergPartitionStatsResult] = {
    loadIcebergTable(fullTableName).flatMap { table =>
      val tableSpec = Option(table.spec())

      if (tableSpec.isEmpty) {
        None
      } else if (!tableSpec.get.isPartitioned) {
        IcebergClusteredStatsExtractor.extractWithRowCounts(fullTableName, table, confName, range)
      } else {
        val partitionAccumulators = buildPartitionAccumulators(table, confName, range)
        Some(resultFromAccumulators(partitionAccumulators))
      }
    }
  }

  private def buildPartitionAccumulators(table: org.apache.iceberg.Table,
                                         confName: String,
                                         range: Option[PartitionRange])(implicit
      partitionSpec: PartitionSpec): mutable.Map[PartitionKey, PartitionAccumulator] = {
    val partitionAccumulators = mutable.Map[PartitionKey, PartitionAccumulator]()

    Iceberg.currentDataFiles(table, includeColumnStats = true, filter = rangeFilterExpression(table.schema(), range)) {
      files =>
        val schema = Option(table.schema())
          .getOrElse(throw new IllegalStateException("Table schema is null"))
        val specs = Option(table.specs())
          .getOrElse(throw new IllegalStateException("Table specs is null"))

        files.foreach { file =>
          val rowCount: Long = file.recordCount()
          val icebergPartitionSpec: org.apache.iceberg.PartitionSpec = Option(specs.get(file.specId()))
            .getOrElse(throw new IllegalStateException(s"Partition spec not found for specId: ${file.specId()}"))
          val partitionFieldIds = Option(icebergPartitionSpec.fields())
            .map(_.asScala.map(_.sourceId()).toSet)
            .getOrElse(Set.empty[Int])

          // Extract partition key using Iceberg's partitionToPath which properly formats all types
          val partition = Option(file.partition())
            .getOrElse(throw new IllegalStateException("File partition data is null"))
          val partitionPath = icebergPartitionSpec.partitionToPath(partition)

          val partitionColToValue: PartitionKey = Iceberg.parsePartitionPath(partitionPath)

          if (partitionKeyInRange(partitionColToValue, range)) {
            val columnStats = extractColumnStats(file, schema, partitionFieldIds)
            val accumulator = partitionAccumulators.getOrElseUpdate(
              partitionColToValue,
              new PartitionAccumulator(partitionColToValue, confName, schema)
            )
            accumulator.addFileStats(rowCount, columnStats)
          }
        }
    }

    partitionAccumulators
  }

  private def resultFromAccumulators(
      accumulators: mutable.Map[PartitionKey, PartitionAccumulator]
  ): IcebergPartitionStatsResult = {
    val tileSummaries = accumulators.values.flatMap(_.toTileSummaries).toMap
    val rowCounts = accumulators.iterator.map { case (key, acc) => key -> acc.totalRowCount }.toMap
    IcebergPartitionStatsResult(tileSummaries, rowCounts)
  }

  private def extractColumnStats(
      file: DataFile,
      schema: org.apache.iceberg.Schema,
      partitionFieldIds: Set[Int]
  ): Map[Int, ColumnStats] = {

    val columnStatsMap = mutable.Map[Int, ColumnStats]()

    // Extract null counts
    val nullCounts = Option(file.nullValueCounts())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .map { case (fieldId, nullCount) => fieldId.toInt -> nullCount.toLong }
          .toMap)
      .getOrElse(Map.empty[Int, Long])

    // Extract lower bounds (min values)
    val lowerBounds = Option(file.lowerBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> Iceberg.boundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Extract upper bounds (max values)
    val upperBounds = Option(file.upperBounds())
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => partitionFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).filter(_ != null).map { validBound =>
                fieldId.toInt -> Iceberg.boundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])

    // Combine all statistics using fieldIds as keys
    // This ensures deterministic ordering since we use fieldId as the key
    val allFieldIds =
      (nullCounts.keySet ++ lowerBounds.keySet ++ upperBounds.keySet)

    allFieldIds.foreach { fieldId =>
      val nullCount = nullCounts.getOrElse(fieldId, 0L)
      val minValue = lowerBounds.get(fieldId)
      val maxValue = upperBounds.get(fieldId)

      val distinctCount: Option[Long] = None

      columnStatsMap(fieldId) = ColumnStats(
        nullCount = nullCount,
        distinctCount = distinctCount,
        minValue = minValue,
        maxValue = maxValue
      )
    }

    columnStatsMap.toMap
  }
}
