package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.iceberg.{DataFile, TableScan}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Type
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.nio.ByteBuffer
import java.time.{LocalDate, ZoneOffset}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case object Iceberg extends Format {
  private[spark] type PartitionKey = List[(String, String)]

  override def tableTypeString: String = "iceberg"

  override def tableProperties: Map[String, String] = {
    Map(
      "commit.retry.num-retries" -> "20", // default = 4
      "commit.retry.min-wait-ms" -> (10 * 1000).toString,
      "commit.retry.max-wait-ms" -> (600 * 1000).toString,
      "commit.status-check.num-retries" -> "20", // default = 3
      "commit.status-check.min-wait-ms" -> (10 * 1000).toString, // default = 1000
      "commit.status-check.max-wait-ms" -> (600 * 1000).toString,
      "write.merge.isolation-level" -> "snapshot",
      "format-version" -> "2"
    )
  }

  def qualifyWithCatalog(tableName: String)(implicit sparkSession: SparkSession): String =
    Format.resolveTableName(tableName).quoted

  private[spark] def parsePartitionPath(partitionPath: String): PartitionKey =
    partitionPath
      .split("/")
      .map { pair =>
        val parts = pair.split("=", 2)
        if (parts.length == 2) {
          parts(0) -> parts(1)
        } else {
          throw new IllegalStateException(s"Invalid partition format: $pair in path $partitionPath")
        }
      }
      .toList

  private[spark] def boundValue(bound: ByteBuffer, fieldType: Type): Any = {
    require(bound != null, "bound cannot be null")
    require(fieldType != null, "fieldType cannot be null")
    org.apache.iceberg.types.Conversions.fromByteBuffer(fieldType, bound)
  }

  private[spark] def partitionMillis(value: Any, fieldType: Type, partitionSpec: PartitionSpec): Long =
    fieldType.typeId() match {
      case Type.TypeID.TIMESTAMP =>
        Math.floorDiv(value.asInstanceOf[java.lang.Long].longValue(), 1000L)
      case Type.TypeID.DATE =>
        LocalDate
          .ofEpochDay(value.asInstanceOf[java.lang.Integer].longValue())
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli
      case Type.TypeID.STRING =>
        partitionSpec.epochMillis(value.toString)
      case other =>
        throw new IllegalArgumentException(s"Unsupported Iceberg bound type $other for value $value")
    }

  private[spark] def partitionValue(value: Any, fieldType: Type, partitionSpec: PartitionSpec): String =
    partitionSpec.at(partitionMillis(value, fieldType, partitionSpec))

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    def fallback: List[String] =
      filterStringRange(sparkMetadataPartitions(tableName, partitionColumn),
                        partitionRange(partitionColumn, partitionFilters))

    Try(loadIcebergTable(tableName).flatMap(listPartitions(_, partitionColumn, partitionFilters))) match {
      case Success(Some(partitions)) => partitions
      case Success(None)             => fallback
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Iceberg partitions directly for $tableName.$partitionColumn: ${Option(e.getMessage).getOrElse("(no message)")}")
        fallback
    }
  }

  private def sparkMetadataPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {
    Try(getIcebergPartitions(tableName, partitionColumn)) match {
      case Success(p) => p
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to get partitions for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }
  }

  private[catalog] def listPartitions(table: org.apache.iceberg.Table,
                                      partitionColumn: String,
                                      partitionFilters: String): Option[List[String]] = {
    val specs = Option(table.specs()).getOrElse {
      throw new IllegalStateException("Could not load Iceberg partition specs")
    }
    val tablePartitionFieldNames = partitionFieldNames(table)

    if (!tablePartitionFieldNames.contains(partitionColumn)) {
      Some(List.empty)
    } else if (tablePartitionFieldNames.contains("hr")) {
      None
    } else {
      val range = partitionRange(partitionColumn, partitionFilters)
      Some(currentDataFiles(table, filter = rangeExpression(table, partitionColumn, range)) { files =>
        val partitions = files.flatMap { file =>
          val spec = Option(specs.get(file.specId())).getOrElse(table.spec())
          partitionFieldValue(spec, file, partitionColumn)
        }.toList

        filterStringRange(Format.sanitizePartitionValues(partitions).distinct, range)
      }.getOrElse(List.empty))
    }
  }

  private def partitionFieldValue(spec: org.apache.iceberg.PartitionSpec,
                                  file: DataFile,
                                  partitionColumn: String): Option[String] = {
    val partition = Option(file.partition()).getOrElse {
      throw new IllegalStateException("File partition data is null")
    }
    val fields = spec.fields().asScala
    val fieldIndex = fields.indexWhere(_.name() == partitionColumn)

    if (fieldIndex < 0) None
    else {
      val field = fields(fieldIndex)
      val fieldType = spec.partitionType().fields().get(fieldIndex).`type`()
      val valueClass = spec.javaClasses()(fieldIndex).asInstanceOf[Class[AnyRef]]
      Option(partition.get(fieldIndex, valueClass)).map { value =>
        field
          .transform()
          .asInstanceOf[Transform[AnyRef, AnyRef]]
          .toHumanString(fieldType, value)
      }
    }
  }

  private def partitionFieldNames(table: org.apache.iceberg.Table): Seq[String] = {
    val currentSpec = Option(table.spec()).toSeq
    val historicalSpecs = Option(table.specs())
      .map(_.values().asScala.toSeq.sortBy(_.specId()))
      .getOrElse(Seq.empty)

    (currentSpec ++ historicalSpecs).flatMap(_.fields().asScala.map(_.name())).distinct
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    val partitionColumnNames = partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames

    partitionsDf
      .select(col("partition"))
      .collect()
      .map { row =>
        val partitionData = row.getStruct(0)
        partitionColumnNames.flatMap { colName =>
          Option(partitionData.getAs[Any](colName)).map(colName -> _.toString)
        }.toMap
      }
      .toList
      .distinct
  }

  /** Partition column names from the Iceberg partition spec, in declaration order. Empty if
    * unpartitioned; the Spark catalog's listColumns cannot see these fields.
    */
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    loadIcebergTable(tableName)
      .flatMap(table => Try(partitionFieldNames(table)).toOption)
      .getOrElse(Seq.empty)

  override def scanDistinctPartitions(tableName: String, partitionColumn: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[String] = {
    Try(loadIcebergTable(tableName).flatMap(listPartitionsByStats(_, partitionColumn, partitionFilters))) match {
      case Success(Some(partitions)) =>
        logger.info(s"Resolved Iceberg distinct file stats for $tableName.$partitionColumn")
        partitions
      case Success(None) =>
        logger.info(
          s"Iceberg distinct file stats were incomplete for $tableName.$partitionColumn; falling back to scan")
        super.scanDistinctPartitions(tableName, partitionColumn, partitionFilters)
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Iceberg distinct file stats for $tableName.$partitionColumn: ${Option(e.getMessage).getOrElse("(no message)")}")
        super.scanDistinctPartitions(tableName, partitionColumn, partitionFilters)
    }
  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] =
    metadataPartitions(tableName, timestampColumn)
      .filter(_.nonEmpty)
      .orElse(statsVirtualPartitions(tableName, timestampColumn, partitionSpec))
      .getOrElse(super.virtualPartitions(tableName, timestampColumn, partitionSpec))

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
    fileStatsDateRange(tableName, columnName, partitionSpec).map { case (range, fieldType) =>
      fieldType.typeId() match {
        case Type.TypeID.TIMESTAMP =>
          partitionSpec.expandRange(range.firstAvailablePartition, partitionSpec.before(range.lastAvailablePartition))
        case _ => range.virtualPartitions(partitionSpec)
      }
    }

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] =
    fileStatsDateRange(tableName, columnName, partitionSpec).map(_._1)

  private def fileStatsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[(StatsDateRange, Type)] =
    Try {
      val table = loadIcebergTable(tableName).getOrElse {
        throw new IllegalStateException(s"Could not load Iceberg table: $tableName")
      }
      val field = Option(table.schema().findField(columnName)).getOrElse {
        throw new IllegalArgumentException(s"Column $columnName not found in Iceberg schema for $tableName")
      }
      val fieldId = field.fieldId().asInstanceOf[java.lang.Integer]
      val fieldType = field.`type`()
      currentDataFilesDateRange(table, fieldId, fieldType, partitionSpec).map(_ -> fieldType)
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Iceberg file stats boundaries for $tableName.$columnName: ${result.get._1}")
        } else {
          logger.info(s"Iceberg file stats were incomplete for $tableName.$columnName")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Iceberg file stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }

  private def fileDateRange(file: DataFile,
                            fieldId: java.lang.Integer,
                            fieldType: org.apache.iceberg.types.Type,
                            partitionSpec: PartitionSpec): Option[(Long, Long)] = {
    val lower = Option(file.lowerBounds()).flatMap(bounds => Option(bounds.get(fieldId)))
    val upper = Option(file.upperBounds()).flatMap(bounds => Option(bounds.get(fieldId)))

    for {
      lowerBound <- lower
      upperBound <- upper
    } yield {
      val lowerMillis = partitionMillis(boundValue(lowerBound, fieldType), fieldType, partitionSpec)
      val upperMillis = partitionMillis(boundValue(upperBound, fieldType), fieldType, partitionSpec)
      lowerMillis -> upperMillis
    }
  }

  private[catalog] def listPartitionsByStats(table: org.apache.iceberg.Table,
                                             partitionColumn: String,
                                             partitionFilters: String): Option[List[String]] = {
    val field = Option(table.schema().findField(partitionColumn)).getOrElse {
      throw new IllegalArgumentException(s"Column $partitionColumn not found in Iceberg schema")
    }

    if (field.`type`().typeId() != Type.TypeID.STRING) {
      None
    } else {
      val range = partitionRange(partitionColumn, partitionFilters)
      listPartitionsByStats(table,
                            partitionColumn,
                            field.fieldId().asInstanceOf[java.lang.Integer],
                            field.`type`(),
                            range)
    }
  }

  private def listPartitionsByStats(table: org.apache.iceberg.Table,
                                    partitionColumn: String,
                                    fieldId: java.lang.Integer,
                                    fieldType: org.apache.iceberg.types.Type,
                                    range: StringRange): Option[List[String]] =
    currentDataFiles(table, includeColumnStats = true, filter = rangeExpression(table, partitionColumn, range)) {
      files =>
        files
          .foldLeft(Some(Set.empty[String]): Option[Set[String]]) {
            case (None, _) => None
            case (Some(acc), file) =>
              singlePartitionValue(file, fieldId, fieldType).map(acc + _)
          }
          .map(values => filterStringRange(Format.sanitizePartitionValues(values).distinct, range))
    }.flatten

  private def singlePartitionValue(file: DataFile,
                                   fieldId: java.lang.Integer,
                                   fieldType: org.apache.iceberg.types.Type): Option[String] = {
    val lower = Option(file.lowerBounds()).flatMap(bounds => Option(bounds.get(fieldId)))
    val upper = Option(file.upperBounds()).flatMap(bounds => Option(bounds.get(fieldId)))

    (lower, upper) match {
      case (Some(lowerBound), Some(upperBound)) =>
        val lowerValue = boundValue(lowerBound, fieldType).toString
        val upperValue = boundValue(upperBound, fieldType).toString
        if (lowerValue == upperValue) Some(lowerValue) else None
      case _ => None
    }
  }

  private[spark] def currentDataFiles[A](table: org.apache.iceberg.Table,
                                         includeColumnStats: Boolean = false,
                                         filter: Option[Expression] = None)(f: Iterator[DataFile] => A): Option[A] =
    Option(table.currentSnapshot()).map { _ =>
      val baseScan = if (includeColumnStats) table.newScan().includeColumnStats() else table.newScan()
      val scan = filter.map(baseScan.filter).getOrElse(baseScan)
      withPlannedDataFiles(scan)(f)
    }

  private def withPlannedDataFiles[A](scan: TableScan)(f: Iterator[DataFile] => A): A = {
    val tasks = scan.planFiles()
    try {
      f(tasks.iterator().asScala.map(_.file()))
    } finally {
      tasks.close()
    }
  }

  private def rangeExpression(table: org.apache.iceberg.Table,
                              columnName: String,
                              range: StringRange): Option[Expression] = {
    val stringColumn = Option(table.schema().findField(columnName))
      .exists(field => field.`type`().typeId() == Type.TypeID.STRING)

    if (!stringColumn) None
    else {
      val expressions: Seq[Expression] =
        range.start.map(Expressions.greaterThanOrEqual(columnName, _)).toSeq ++
          range.endInclusive.map(Expressions.lessThanOrEqual(columnName, _)).toSeq ++
          range.endExclusive.map(Expressions.lessThan(columnName, _)).toSeq
      expressions.reduceOption((left, right) => Expressions.and(left, right))
    }
  }

  private case class StringRange(start: Option[String], endInclusive: Option[String], endExclusive: Option[String])

  private val RangePredicate = """(?i)`?([^`\s()]+)`?\s*(>=|<=|<)\s*'([^']+)'""".r

  private def partitionRange(partitionColumn: String, partitionFilters: String): StringRange = {
    val predicates = RangePredicate
      .findAllMatchIn(partitionFilters)
      .collect {
        case predicate if predicate.group(1) == partitionColumn => predicate.group(2) -> predicate.group(3)
      }
      .toSeq
    StringRange(
      start = predicates.collectFirst { case (">=", value) => value },
      endInclusive = predicates.collectFirst { case ("<=", value) => value },
      endExclusive = predicates.collectFirst { case ("<", value) => value }
    )
  }

  private def filterStringRange(values: List[String], range: StringRange): List[String] =
    values.filter(value =>
      range.start.forall(value >= _) &&
        range.endInclusive.forall(value <= _) &&
        range.endExclusive.forall(value < _))

  private def currentDataFilesDateRange(table: org.apache.iceberg.Table,
                                        fieldId: java.lang.Integer,
                                        fieldType: org.apache.iceberg.types.Type,
                                        partitionSpec: PartitionSpec): Option[StatsDateRange] =
    currentDataFiles(table, includeColumnStats = true) { files =>
      StatsDateRange.fromFileStats(
        files.map(fileDateRange(_, fieldId, fieldType, partitionSpec)),
        partitionSpec,
        endMillis =>
          if (fieldType.typeId() == Type.TypeID.TIMESTAMP) Format.readinessPartition(endMillis, partitionSpec)
          else partitionSpec.at(endMillis)
      )
    }.flatten

  private[catalog] def loadIcebergTable(tableName: String)(implicit
      sparkSession: SparkSession): Option[org.apache.iceberg.Table] =
    Try {
      val resolved = Format.resolveTableName(tableName)
      val catalog = sparkSession.sessionState.catalogManager
        .catalog(resolved.catalog)
        .asInstanceOf[TableCatalog]

      catalog.loadTable(resolved.toIdentifier) match {
        case sparkTable: SparkTable => sparkTable.table()
        case other => throw new IllegalStateException(s"Not an Iceberg SparkTable: ${other.getClass.getName}")
      }
    }.toOption

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.table(s"${Format.resolveTableName(tableName).quoted}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .flatMap(row => Option(row.getString(0)))
        .toList
    } else {
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"))
        .collect()
        .flatMap(row => Option(row.getString(0)))
        .toList
    }
  }

  override def supportSubPartitionsFilter: Boolean = false
}
