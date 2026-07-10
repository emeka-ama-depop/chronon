package ai.chronon.spark.batch

import ai.chronon.api.planner.{MetaDataUtils, TableDependencies}
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, Query, TimeUnit, Window}
import ai.chronon.planner.Node
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.other.MockKVStore
import ai.chronon.spark.utils.{MockApi, SparkTestBase}
import org.scalatest.matchers.should.Matchers

class BatchNodeRunnerStatsCompatibilityTest extends SparkTestBase with Matchers {

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.allFilesInCrc.thresholdNumFiles" -> "0"
  )

  "BatchNodeRunner.computeInputTablePartitionStatuses" should
    "preserve exact-boundary readiness and first-partition diagnostics on the driver" in {
      val suffix = java.lang.Long.toUnsignedString(System.nanoTime())
      val namespace = s"stats_compatibility_$suffix"
      val table = s"$namespace.events"
      val threeHourSpec = PartitionSpec("created_at", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)
      val tableUtils = new TableUtils(spark, Some(threeHourSpec))

      spark.sql(s"CREATE DATABASE $namespace")

      try {
        spark.sql(s"""
          CREATE TABLE $table (
            created_at TIMESTAMP,
            id INT
          ) USING delta
        """)
        spark.sql(s"""
          INSERT INTO $table VALUES
            (TIMESTAMP '2024-01-01 00:00:00', 1),
            (TIMESTAMP '2024-01-01 06:00:00', 2)
        """)

        val dependency = TableDependencies.fromTable(
          table,
          new Query()
            .setPartitionColumn(threeHourSpec.column)
            .setPartitionFormat(threeHourSpec.format)
            .setPartitionInterval(new Window(3, TimeUnit.HOURS))
            .setTimePartitioned(true)
        )
        implicit val partitionSpec: PartitionSpec = threeHourSpec
        val metadata = MetaDataUtils.layer(
          baseMetadata = new MetaData().setOutputNamespace(namespace).setTeam("test"),
          modeName = "backfill",
          nodeName = "stats_compatibility",
          tableDependencies = Seq(dependency),
          stepDays = Some(1),
          outputTableOverride = Some(s"$namespace.output")
        )
        val runner = new BatchNodeRunner(
          new Node().setMetaData(metadata),
          tableUtils,
          new MockApi(() => new MockKVStore(), "test")
        )
        val range = PartitionRange("2024-01-01-06-00", "2024-01-01-06-00")
        val jobGroup = s"stats-compatibility-$suffix"

        spark.sparkContext.setJobGroup(jobGroup, "driver-only stats compatibility")
        val statuses = try {
          runner.computeInputTablePartitionStatuses(metadata, range, tableUtils).toSeq
        } finally {
          spark.sparkContext.clearJobGroup()
        }

        statuses should have size 1
        val status = statuses.head
        status.ready shouldBe false
        status.firstAvailablePartition shouldBe Some("2024-01-01-00-00")
        status.lastAvailablePartition shouldBe Some("2024-01-01-03-00")
        val submittedJobs = spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).toSeq
        withClue(s"readiness submitted Spark jobs: ${submittedJobs.mkString(", ")}") {
          submittedJobs shouldBe empty
        }
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $table")
        spark.sql(s"DROP DATABASE IF EXISTS $namespace")
      }
    }
}
