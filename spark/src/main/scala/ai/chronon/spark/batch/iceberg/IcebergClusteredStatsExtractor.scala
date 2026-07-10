package ai.chronon.spark.batch.iceberg

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.observability.{TileSummary, TileSummaryKey}
import ai.chronon.spark.batch.iceberg.IcebergPartitionStatsExtractor.IcebergPartitionStatsResult
import ai.chronon.spark.catalog.Iceberg
import org.apache.iceberg.DataFile
import org.apache.iceberg.types.Type
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object IcebergClusteredStatsExtractor {
  import IcebergPartitionStatsExtractor.PartitionKey

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  def extract(fullTableName: String,
              table: org.apache.iceberg.Table,
              confName: String,
              range: Option[PartitionRange] = None)(implicit
      partitionSpec: PartitionSpec): Option[Map[TileSummaryKey, TileSummary]] = {
    extractWithRowCounts(fullTableName, table, confName, range).map(_.tileSummaries)
  }

  def extractWithRowCounts(fullTableName: String,
                           table: org.apache.iceberg.Table,
                           confName: String,
                           range: Option[PartitionRange] = None)(implicit
      partitionSpec: PartitionSpec): Option[IcebergPartitionStatsResult] = {
    Option(table.schema()) match {
      case None =>
        logger.info(
          s"Cannot extract synthetic partition stats for $fullTableName because the Iceberg schema is missing")
        None
      case Some(schema) =>
        Option(schema.findField(partitionSpec.column)) match {
          case None =>
            logger.info(
              s"Cannot extract synthetic partition stats for $fullTableName because column ${partitionSpec.column} is missing")
            None
          case Some(partitionField) =>
            extractForPartitionField(fullTableName,
                                     table,
                                     confName,
                                     schema,
                                     partitionField.fieldId(),
                                     partitionField.`type`(),
                                     range)
        }
    }
  }

  private def extractForPartitionField(fullTableName: String,
                                       table: org.apache.iceberg.Table,
                                       confName: String,
                                       schema: org.apache.iceberg.Schema,
                                       partitionFieldId: Int,
                                       partitionFieldType: Type,
                                       range: Option[PartitionRange])(implicit
      partitionSpec: PartitionSpec): Option[IcebergPartitionStatsResult] = {
    Iceberg
      .currentDataFiles(table,
                        includeColumnStats = true,
                        filter = IcebergPartitionStatsExtractor.rangeFilterExpression(table.schema(), range)) { files =>
        val partitionAccumulators = mutable.Map[PartitionKey, PartitionAccumulator]()
        val complete = files.forall { file =>
          val partitionKey = syntheticPartitionKey(file, partitionFieldId, partitionFieldType)
          val columnStats = extractStrictColumnStats(file, schema, Set(partitionFieldId))

          (partitionKey, columnStats) match {
            case (Some(key), Some(stats)) =>
              val accumulator = partitionAccumulators.getOrElseUpdate(
                key,
                new PartitionAccumulator(key, confName, schema)
              )
              accumulator.addFileStats(file.recordCount(), stats)
              true
            case _ =>
              false
          }
        }

        if (complete) {
          val tileSummaries = partitionAccumulators.values.flatMap(_.toTileSummaries).toMap
          val rowCounts = partitionAccumulators.iterator.map { case (key, acc) =>
            key -> acc.totalRowCount
          }.toMap
          Some(IcebergPartitionStatsResult(tileSummaries, rowCounts))
        } else None
      }
      .getOrElse(Some(IcebergPartitionStatsResult(Map.empty, Map.empty)))
  }

  private def syntheticPartitionKey(file: DataFile, partitionFieldId: Int, partitionFieldType: Type)(implicit
      partitionSpec: PartitionSpec): Option[PartitionKey] = {
    for {
      lower <- partitionBoundValue(file.lowerBounds(), partitionFieldId, partitionFieldType)
      upper <- partitionBoundValue(file.upperBounds(), partitionFieldId, partitionFieldType)
      if lower == upper
    } yield List(partitionSpec.column -> lower)
  }

  private def partitionBoundValue(bounds: java.util.Map[Integer, java.nio.ByteBuffer],
                                  partitionFieldId: Int,
                                  partitionFieldType: Type)(implicit partitionSpec: PartitionSpec): Option[String] =
    Option(bounds).flatMap { values =>
      Option(values.get(partitionFieldId)).flatMap { bound =>
        Try(
          Iceberg.partitionValue(Iceberg.boundValue(bound, partitionFieldType),
                                 partitionFieldType,
                                 partitionSpec)).toOption
      }
    }

  private[iceberg] def extractStrictColumnStats(file: DataFile,
                                                schema: org.apache.iceberg.Schema,
                                                excludedFieldIds: Set[Int]): Option[Map[Int, ColumnStats]] = {
    val fieldIds = schema.columns().asScala.map(_.fieldId()).filterNot(excludedFieldIds.contains)
    val nullCounts = Option(file.nullValueCounts()).map(
      _.asScala
        .map { case (fieldId, nullCount) =>
          fieldId.toInt -> nullCount.toLong
        }
        .toMap)
    val lowerBounds = extractBounds(file.lowerBounds(), schema, excludedFieldIds)
    val upperBounds = extractBounds(file.upperBounds(), schema, excludedFieldIds)

    nullCounts.flatMap { counts =>
      if (fieldIds.forall(counts.contains)) {
        Some(fieldIds.map { fieldId =>
          fieldId -> ColumnStats(
            nullCount = counts(fieldId),
            minValue = lowerBounds.get(fieldId),
            maxValue = upperBounds.get(fieldId)
          )
        }.toMap)
      } else {
        None
      }
    }
  }

  private def extractBounds(bounds: java.util.Map[Integer, java.nio.ByteBuffer],
                            schema: org.apache.iceberg.Schema,
                            excludedFieldIds: Set[Int]): Map[Int, Any] =
    Option(bounds)
      .map(
        _.asScala
          .filterNot { case (fieldId, _) => excludedFieldIds.contains(fieldId) }
          .flatMap { case (fieldId, bound) =>
            Option(schema.findField(fieldId)).flatMap { field =>
              Option(bound).map { validBound =>
                fieldId.toInt -> Iceberg.boundValue(validBound, field.`type`())
              }
            }
          }
          .toMap)
      .getOrElse(Map.empty[Int, Any])
}
