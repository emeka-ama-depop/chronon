package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.delta.kernel.{Table => KernelTable}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.{InternalScanFileUtils, ScanImpl}
import io.delta.kernel.internal.util.ColumnMapping
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Table, TableCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  NumericType,
  StringType,
  StructType,
  TimestampNTZType,
  TimestampType
}
import org.apache.spark.unsafe.types.UTF8String

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

// Delta Kernel is kept at the same version as the EMR-bundled delta-spark runtime. Active-file
// metadata is read through Kernel so readiness does not materialize DeltaLog DataFrames.
case object DeltaLake extends Format {

  private val objectMapper = new ObjectMapper()
  private case class TableMetadata(path: String, schema: StructType, partitionColumns: Seq[String])

  override def tableTypeString: String = "delta"

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] =
    activePartitions(tableMetadataOrThrow(tableName))

  private def activePartitions(deltaTable: TableMetadata)(implicit
      sparkSession: SparkSession): List[Map[String, String]] =
    withScanFiles(deltaTable, includeStats = false) { (engine, snapshot, batches) =>
      val partitionColumns = snapshot.getPartitionColumnNames(engine).asScala.map { logicalName =>
        logicalName -> ColumnMapping.getPhysicalName(snapshot.getSchema(engine).get(logicalName))
      }

      batches.asScala
        .flatMap { batch =>
          val rows = batch.getRows
          try {
            rows.asScala.map { file =>
              val partitionValues = InternalScanFileUtils.getPartitionValues(file)
              partitionColumns.map { case (logicalName, physicalName) =>
                logicalName -> partitionValues.get(physicalName)
              }.toMap
            }.toList
          } finally {
            rows.close()
          }
        }
        .toList
        .distinct
    }

  // the spark catalog's listColumns doesn't expose partitioning for delta tables; the delta
  // log's own metadata is the ordered source of truth
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    tableMetadata(tableName).map(_.partitionColumns).getOrElse(Seq.empty)

  override protected def metadataPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): Option[List[String]] =
    tableMetadata(tableName).flatMap { deltaTable =>
      if (!deltaTable.partitionColumns.contains(partitionColumn)) {
        Some(List.empty)
      } else {
        Try {
          activePartitions(deltaTable)
            .flatMap(_.get(partitionColumn).flatMap(Option(_)))
            .distinct
        } match {
          case Success(partitions) => Some(partitions)
          case Failure(e) =>
            logger.warn(
              s"Failed to resolve Delta partition metadata for $tableName.$partitionColumn: ${Option(e.getMessage).getOrElse("(no message)")}")
            None
        }
      }
    }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsVirtualPartitions(tableName, timestampColumn, partitionSpec))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))
  }

  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    metadataFirstAvailablePartition(tableName, partitionColumn)
      .orElse(statsDateRange(tableName, partitionColumn, partitionSpec).map(_.firstAvailablePartition))
      .orElse(scanFirstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    metadataLastAvailablePartition(tableName, partitionColumn)
      .orElse(statsLastAvailablePartition(tableName, partitionColumn, partitionSpec))
      .orElse(scanLastAvailablePartition(tableName, partitionColumn, partitionSpec))

  private def statsLastAvailablePartition(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    statsDateRange(tableName, columnName, partitionSpec).map(_.lastAvailablePartition)

  private def statsVirtualPartitions(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[List[String]] =
    fileStatsDateRange(tableName, columnName, partitionSpec).map { case (range, columnType) =>
      columnType match {
        case TimestampType =>
          partitionSpec.expandRange(range.firstAvailablePartition, partitionSpec.before(range.lastAvailablePartition))
        case TimestampNTZType =>
          partitionSpec.expandRange(range.firstAvailablePartition, partitionSpec.before(range.lastAvailablePartition))
        case _ => range.virtualPartitions(partitionSpec)
      }
    }

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] =
    fileStatsDateRange(tableName, columnName, partitionSpec).map(_._1)

  private def fileStatsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[(StatsDateRange, DataType)] = {
    Try {
      val deltaTable = tableMetadataOrThrow(tableName)
      val field = deltaTable.schema(columnName)
      withScanFiles(deltaTable, includeStats = true) { (engine, snapshot, batches) =>
        val physicalColumnName = ColumnMapping.getPhysicalName(snapshot.getSchema(engine).get(columnName))
        StatsDateRange
          .fromFileStats(
            batches.asScala.flatMap { batch =>
              val rows = batch.getRows
              try {
                rows.asScala.map(fileDateRange(_, physicalColumnName, field.dataType, partitionSpec)).toList
              } finally {
                rows.close()
              }
            },
            partitionSpec,
            endMillis =>
              field.dataType match {
                case TimestampType | _: NumericType => Format.readinessPartition(endMillis, partitionSpec)
                case _                              => partitionSpec.at(endMillis)
              }
          )
          .map(_ -> field.dataType)
      }
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Delta file stats boundaries for $tableName.$columnName: ${result.get._1}")
        } else {
          logger.warn(s"Delta file stats were incomplete for $tableName.$columnName")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Delta file stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  private def fileDateRange(file: io.delta.kernel.data.Row,
                            columnName: String,
                            columnType: DataType,
                            partitionSpec: PartitionSpec): Option[(Long, Long)] = {
    val partitionValue = Option(InternalScanFileUtils.getPartitionValues(file).get(columnName))
    partitionValue
      .flatMap(value => boundaryMillis(value, columnType, partitionSpec).map(millis => millis -> millis))
      .orElse {
        val addFile = file.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL)
        val stats =
          if (addFile.isNullAt(InternalScanFileUtils.ADD_FILE_STATS_ORDINAL)) None
          else Some(objectMapper.readTree(addFile.getString(InternalScanFileUtils.ADD_FILE_STATS_ORDINAL)))
        stats.flatMap { stats =>
          for {
            lower <- statsValue(stats, "minValues", columnName)
            upper <- statsValue(stats, "maxValues", columnName)
            lowerMillis <- boundaryMillis(lower, columnType, partitionSpec)
            upperMillis <- boundaryMillis(upper, columnType, partitionSpec)
          } yield lowerMillis -> upperMillis
        }
      }
  }

  private def statsValue(stats: JsonNode, boundary: String, columnName: String): Option[JsonNode] =
    Option(stats.path(boundary).get(columnName)).filterNot(_.isNull)

  private def boundaryMillis(value: JsonNode, columnType: DataType, partitionSpec: PartitionSpec): Option[Long] =
    if (value == null || value.isNull) None else boundaryMillis(value.asText(), columnType, partitionSpec)

  private def boundaryMillis(value: String, columnType: DataType, partitionSpec: PartitionSpec): Option[Long] =
    Try {
      columnType match {
        case DateType         => LocalDate.parse(value).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli
        case StringType       => parsePartitionOrTimestamp(value, partitionSpec)
        case _: NumericType   => BigDecimal(value).toLong
        case TimestampType    => parseTimestamp(value)
        case TimestampNTZType => LocalDateTime.parse(value.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli
        case other            => throw new IllegalArgumentException(s"Unsupported Delta stats type $other")
      }
    }.toOption

  private def parseTimestamp(value: String): Long =
    DateTimeUtils
      .stringToTimestamp(UTF8String.fromString(value), ZoneOffset.UTC)
      .map(micros => Math.floorDiv(micros, 1000L))
      .getOrElse(throw new IllegalArgumentException(s"Invalid timestamp: $value"))

  private def parsePartitionOrTimestamp(value: String, partitionSpec: PartitionSpec): Long =
    Try(partitionSpec.epochMillis(value)).getOrElse(parseTimestamp(value))

  private def tableMetadataOrThrow(tableName: String)(implicit sparkSession: SparkSession): TableMetadata =
    tableMetadata(tableName).getOrElse(throw new IllegalArgumentException(s"Not a Delta table: $tableName"))

  private def withScanFiles[A](deltaTable: TableMetadata, includeStats: Boolean)(
      consume: (
          io.delta.kernel.engine.Engine,
          io.delta.kernel.Snapshot,
          io.delta.kernel.utils.CloseableIterator[io.delta.kernel.data.FilteredColumnarBatch]
      ) => A
  )(implicit sparkSession: SparkSession): A = {
    val engine = DefaultEngine.create(sparkSession.sessionState.newHadoopConf())
    val snapshot = KernelTable.forPath(engine, deltaTable.path).getLatestSnapshot(engine)
    // Delta Kernel 3.3's public scan can omit stats. Its includeStats path is the
    // protocol-aware driver reader for both checkpoint and JSON-log file actions.
    val scan = snapshot.getScanBuilder(engine).build().asInstanceOf[ScanImpl]
    val batches = scan.getScanFiles(engine, includeStats)
    try consume(engine, snapshot, batches)
    finally batches.close()
  }

  private def tableMetadata(tableName: String)(implicit sparkSession: SparkSession): Option[TableMetadata] =
    Try {
      val resolved = Format.resolveTableName(tableName)
      val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog).asInstanceOf[TableCatalog]
      catalog.loadTable(resolved.toIdentifier) match {
        case table: DeltaTableV2 =>
          Some(TableMetadata(table.path.toUri.toString, table.schema(), tablePartitionColumnNames(table)))
        case table =>
          val properties = table.properties()
          val deltaProvider = Option(properties.get(TableCatalog.PROP_PROVIDER)).exists(_.equalsIgnoreCase("delta"))
          Option(properties.get(TableCatalog.PROP_LOCATION))
            .filter(_ => deltaProvider)
            .map { path =>
              val schema = StructType(table.columns().map { column =>
                val field = org.apache.spark.sql.types.StructField(column.name(), column.dataType(), column.nullable())
                Option(column.comment()).fold(field)(field.withComment)
              })
              TableMetadata(path, schema, tablePartitionColumnNames(table))
            }
      }
    }.toOption.flatten

  private def tablePartitionColumnNames(table: Table): Seq[String] =
    table
      .partitioning()
      .flatMap(_.references())
      .map(_.fieldNames().mkString("."))
      .toSeq

  private[catalog] def isDeltaTable(tableName: String)(implicit sparkSession: SparkSession): Boolean =
    tableMetadata(tableName).isDefined

  override def supportSubPartitionsFilter: Boolean = true
}
