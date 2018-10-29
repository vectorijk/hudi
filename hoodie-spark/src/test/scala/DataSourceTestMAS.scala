/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

import java.util.UUID

import com.uber.hoodie.common.HoodieTestDataGenerator
import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceWriteOptions, HistoryAvroPayload, HoodieDataSourceHelpers}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

/**
  * Basic tests on the spark datasource
  */
class DataSourceTestMAS extends AssertionsForJUnit {

  var spark: SparkSession = null
  var dataGen: HoodieTestDataGenerator = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY -> classOf[HistoryAvroPayload].getName,
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )
  var basePath: String = null
  var fs: FileSystem = null

  @Before def initialize() {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    dataGen = new HoodieTestDataGenerator()
    val folder = new TemporaryFolder
    folder.create
    basePath = folder.getRoot.getAbsolutePath
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
  }

  @Test def testCopyOnWriteStorage() {
    val uuid1 = UUID.randomUUID.toString
    val uuid2 = UUID.randomUUID.toString
    // Insert Operation
    val col1: String = s"""{"timestamp": 0.0, "_row_key": "${uuid1}", "name": "p1", "location": 1, "partition": "2015/03/16", "history": [-1]}"""
    val col3: String = s"""{"timestamp": 1.0, "_row_key": "${uuid1}", "name": "p1", "location": 3, "partition": "2015/03/16", "history": [-1]}"""
    val col2: String = s"""{"timestamp": 0.0, "_row_key": "${uuid2}", "name": "p2", "location": 2, "partition": "2015/03/16", "history": [-1]}"""
    val col4: String = s"""{"timestamp": 1.0, "_row_key": "${uuid2}", "name": "p2", "location": 5, "partition": "2015/03/16", "history": [-1]}"""

//    val tb = List(col1, col2)
//    val tb = List(col1, col2, col3)
    val tb = List(col1, col2, col3, col4)
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(tb, 2))
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    inputDF1.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF1.count())
    hoodieROViewDF1.show()

    val col21: String = s"""{"timestamp": 2.0, "_row_key": "${uuid1}", "name": "p1", "location": 12, "partition": "2015/03/16", "history": [-1]}"""
    val col22: String = s"""{"timestamp": 2.0, "_row_key": "${uuid2}", "name": "p2", "location": 14, "partition": "2015/03/16", "history": [-1]}"""
    val col23: String = s"""{"timestamp": 3.0, "_row_key": "${uuid2}", "name": "p2", "location": 11, "partition": "2015/03/16", "history": [-1]}"""
    val col24: String = s"""{"timestamp": 3.0, "_row_key": "${uuid1}", "name": "p1", "location": 15, "partition": "2015/03/16", "history": [-1]}"""
//    val tb2 = List(col21, col22)
//    val tb2 = List(col21, col22, col23)
    val tb2 = List(col21, col22, col23, col24)
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(tb2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.show(100, false)

    // Upsert Operation
    inputDF2.write.format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF2.count()) // still 100, since we only updated

    hoodieROViewDF2.select("history", "location", "name", "partition", "timestamp").show(10, false)
  }
}
