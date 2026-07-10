package ai.chronon.spark.batch

import ai.chronon.api.planner.{MetaDataUtils, TableDependencies}
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, Query}
import ai.chronon.planner.Node
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.other.MockKVStore
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.matchers.should.Matchers

class BatchNodeRunnerFileStatsReadinessTest extends SparkTestBase with Matchers {

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" ->
      "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.iceberg" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type" -> "hadoop",
    "spark.sql.catalog.iceberg.warehouse" -> icebergWarehouse,
    // Do not let a tiny test table hide a Spark-backed Delta file listing behind the CRC shortcut.
    "spark.databricks.delta.allFilesInCrc.thresholdNumFiles" -> "0"
  )

  "BatchNodeRunner.computeInputTablePartitionStatuses" should
    "use driver file stats for Delta and Iceberg inputs" in {
      val suffix = java.lang.Long.toUnsignedString(System.nanoTime())
      val namespace = s"driver_readiness_$suffix"
      val deltaTable = s"$namespace.delta_events"
      val partitionedDeltaTable = s"$namespace.partitioned_delta_events"
      val icebergTable = s"iceberg.$namespace.iceberg_events"
      val tableUtils = new TableUtils(spark)

      spark.sql(s"CREATE DATABASE $namespace")
      spark.sql(s"CREATE DATABASE iceberg.$namespace")

      try {
        spark.sql(s"""
          CREATE TABLE $deltaTable (
            created_at TIMESTAMP,
            id INT
          ) USING delta
        """)
        spark.sql(s"""
          INSERT INTO $deltaTable VALUES
            (TIMESTAMP '2024-01-01 12:00:00', 1),
            (TIMESTAMP '2024-01-03 12:00:00', 2)
        """)
        DeltaLog.forTable(spark, TableIdentifier("delta_events", Some(namespace))).checkpoint()

        spark.sql(s"""
          CREATE TABLE $partitionedDeltaTable (
            id INT,
            ds STRING
          ) USING delta
          PARTITIONED BY (ds)
        """)
        spark.sql(s"""
          INSERT INTO $partitionedDeltaTable VALUES
            (1, '2024-01-01'),
            (2, '2024-01-03')
        """)

        spark.sql(s"""
          CREATE TABLE $icebergTable (
            created_at TIMESTAMP,
            id INT
          ) USING iceberg
          TBLPROPERTIES ('write.metadata.metrics.default' = 'full')
        """)
        spark.sql(s"""
          INSERT INTO $icebergTable VALUES
            (TIMESTAMP '2024-01-01 12:00:00', 1),
            (TIMESTAMP '2024-01-03 12:00:00', 2)
        """)

        def dependency(table: String, partitionColumn: String = "created_at", timePartitioned: Boolean = true) =
          TableDependencies.fromTable(
            table,
            new Query()
              .setPartitionColumn(partitionColumn)
              .setPartitionFormat("yyyy-MM-dd")
              .setTimePartitioned(timePartitioned)
          )

        implicit val partitionSpec: PartitionSpec = PartitionSpec.daily
        val metadata = MetaDataUtils.layer(
          baseMetadata = new MetaData().setOutputNamespace(namespace).setTeam("test"),
          modeName = "backfill",
          nodeName = "driver_readiness",
          tableDependencies = Seq(
            dependency(deltaTable),
            dependency(partitionedDeltaTable, partitionColumn = "ds", timePartitioned = false),
            dependency(icebergTable)
          ),
          stepDays = Some(1),
          outputTableOverride = Some(s"$namespace.output")
        )
        val runner = new BatchNodeRunner(
          new Node().setMetaData(metadata),
          tableUtils,
          new MockApi(() => new MockKVStore(), "test")
        )
        val range = PartitionRange("2024-01-01", "2024-01-02")
        val jobGroup = s"driver-readiness-$suffix"

        spark.sparkContext.setJobGroup(jobGroup, "driver-only file-stats readiness")
        val statuses = try {
          runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq
        } finally {
          spark.sparkContext.clearJobGroup()
        }

        val byTable = statuses.map(status => status.name -> status).toMap
        byTable.keySet shouldBe Set(deltaTable, partitionedDeltaTable, icebergTable)
        byTable.values.foreach { status =>
          status.ready shouldBe true
          status.firstAvailablePartition shouldBe Some("2024-01-01")
          status.lastAvailablePartition shouldBe Some("2024-01-03")
        }
        val submittedJobs = spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).toSeq
        withClue(s"readiness submitted Spark jobs: ${submittedJobs.mkString(", ")}") {
          submittedJobs shouldBe empty
        }
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $deltaTable")
        spark.sql(s"DROP TABLE IF EXISTS $partitionedDeltaTable")
        spark.sql(s"DROP TABLE IF EXISTS $icebergTable")
        spark.sql(s"DROP DATABASE IF EXISTS $namespace")
        spark.sql(s"DROP DATABASE IF EXISTS iceberg.$namespace")
      }
    }
}
