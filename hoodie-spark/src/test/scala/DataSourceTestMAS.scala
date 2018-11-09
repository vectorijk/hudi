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

import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceWriteOptions, HistoryAvroPayload, HoodieDataSourceHelpers}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

/**
  * Basic tests on the spark datasource for HistoryAvroPayload
  */
class HudiDataSourceTest extends AssertionsForJUnit {
  var spark: SparkSession = null
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
  var dfSchema: StructType = null

  @Before def initialize() {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    val folder = new TemporaryFolder
    folder.create
    basePath = folder.getRoot.getAbsolutePath
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    dfSchema = new StructType()
      .add("timestamp", DoubleType)
      .add("_row_key", StringType)
      .add("name", StringType)
      .add("location", LongType)
      .add("partition", StringType)
      .add("history", ArrayType(StringType, true))
  }

  @Test def testCopyOnWriteStorage() {
    val uuid1 = UUID.randomUUID.toString
    val uuid2 = UUID.randomUUID.toString
    // Insert Operation
    val col1: String =
      s"""{"timestamp": 0.0, "_row_key": "${uuid1}", "name": "p1", "location": 1, "partition": "2015/03/16", "history": []}"""
    val col2: String =
      s"""{"timestamp": 1.0, "_row_key": "${uuid1}", "name": "p1", "location": 3, "partition": "2015/03/16", "history": []}"""
    //    val col3: String =
    //      s"""{"timestamp": 0.0, "_row_key": "${uuid2}", "name": "p2", "location": 2, "partition": "2015/03/16", "history": []}"""
    val col4: String =
    s"""{"timestamp": 1.0, "_row_key": "${uuid2}", "name": "p2", "location": 5, "partition": "2015/03/16", "history": []}"""

    val tb = List(col2, col1, col4)
    val inputDF1: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb, 2))
    inputDF1.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY,
        DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    inputDF1.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    hoodieROViewDF1.show()
    assertEquals(2, hoodieROViewDF1.count())

    val col21: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid1}", "name": "p1", "location": 12, "partition": "2015/03/16", "history": []}"""
    val col22: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid2}", "name": "p2", "location": 14, "partition": "2015/03/16", "history": []}"""
    val col23: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid2}", "name": "p2", "location": 11, "partition": "2015/03/16", "history": []}"""
    val col24: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid1}", "name": "p1", "location": 15, "partition": "2015/03/16", "history": []}"""
    val col25: String =
      s"""{"timestamp": 4.0, "_row_key": "${uuid1}", "name": "p1", "location": 16, "partition": "2015/03/16", "history": []}"""

    val tb2 = List(col21, col22, col23, col24, col25)
    val inputDF2: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.show(100, false)

    // Upsert Operation
    inputDF2.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    assertEquals(2, hoodieROViewDF2.count()) // still 100, since we only updated

    hoodieROViewDF2.select("history", "location", "name", "partition", "timestamp").show(10, false)

    // p1 -> 1, 3, 12, 15
    // p2 -> 2, 5, 11, 14

    //    +------------------+--------+----+----------+---------+
    //    |history           |location|name|partition |timestamp|
    //    +------------------+--------+----+----------+---------+
    //    |[15, 12, 16, 3, 1]|16      |p1  |2015/03/16|4.0      |
    //    |[14, 11, 5, 2]    |11      |p2  |2015/03/16|3.0      |
    //    +------------------+--------+----+----------+---------+
  }

  @Test def testCopyOnWrite3CommitsStorage() {
    val uuid1 = UUID.randomUUID.toString
    val uuid2 = UUID.randomUUID.toString
    // Insert Operation
    val col1: String =
      s"""{"timestamp": 0.0, "_row_key": "${uuid1}", "name": "p1", "location": 1, "partition": "2015/03/16", "history": []}"""
    val col2: String =
      s"""{"timestamp": 1.0, "_row_key": "${uuid1}", "name": "p1", "location": 3, "partition": "2015/03/16", "history": []}"""
    val col3: String =
      s"""{"timestamp": 0.0, "_row_key": "${uuid2}", "name": "p2", "location": 2, "partition": "2015/03/16", "history": []}"""
    //    val col4: String =
    //    s"""{"timestamp": 1.0, "_row_key": "${uuid2}", "name": "p2", "location": 5, "partition": "2015/03/16", "history": []}"""

    val tb = List(col1, col2, col3)
    val inputDF1: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb, 2))
    inputDF1.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY,
        DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    inputDF1.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    hoodieROViewDF1.show()
    assertEquals(2, hoodieROViewDF1.count())

    val col21: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid1}", "name": "p1", "location": 12, "partition": "2015/03/16", "history": []}"""
    val col24: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid1}", "name": "p1", "location": 15, "partition": "2015/03/16", "history": []}"""
    val col25: String =
      s"""{"timestamp": 4.0, "_row_key": "${uuid1}", "name": "p1", "location": 16, "partition": "2015/03/16", "history": []}"""
    val col22: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid2}", "name": "p2", "location": 14, "partition": "2015/03/16", "history": []}"""
    val col23: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid2}", "name": "p2", "location": 11, "partition": "2015/03/16", "history": []}"""
    //    val tb2 = List(col21, col22)
    //    val tb2 = List(col21, col22, col23)
    val tb2 = List(col21, col23, col22, col24, col25)
    val inputDF2: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.show(100, false)

    // Upsert Operation
    inputDF2.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF2.count())

    hoodieROViewDF2.select("history", "location", "name", "partition", "timestamp").show(10, false)

    // p1 -> 1, 3, 12, 15
    // p2 -> 2, 5, 11, 14
    //    +----------------------+--------+----+----------+---------+
    //    |history               |location|name|partition |timestamp|
    //    +----------------------+--------+----+----------+---------+
    //    |[-1, 12, 15, 3, -1, 1]|15      |p1  |2015/03/16|3.0      |
    //    |[-1, 14, 11, 5, -1, 2]|11      |p2  |2015/03/16|3.0      |
    //    +----------------------+--------+----+----------+---------+

    //    +------------------+--------+----+----------+---------+
    //    |history           |location|name|partition |timestamp|
    //    +------------------+--------+----+----------+---------+
    //    |[15, 12, 16, 3, 1]|16      |p1  |2015/03/16|4.0      |
    //    |[14, 11, 5, 2]    |11      |p2  |2015/03/16|3.0      |
    //    +------------------+--------+----+----------+---------+

    val col31: String =
      s"""{"timestamp": 6.0, "_row_key": "${uuid1}", "name": "p1", "location": 20, "partition": "2015/03/16", "history": []}"""
    val col32: String =
      s"""{"timestamp": 5.0, "_row_key": "${uuid1}", "name": "p1", "location": 22, "partition": "2015/03/16", "history": []}"""
    val col33: String =
      s"""{"timestamp": 6.0, "_row_key": "${uuid2}", "name": "p2", "location": 23, "partition": "2015/03/16", "history": []}"""
    val col34: String =
      s"""{"timestamp": 5.0, "_row_key": "${uuid2}", "name": "p2", "location": 21, "partition": "2015/03/16", "history": []}"""
    val tb3 = List(col31, col32, col33, col34)
    val inputDF3: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb3, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF3.show(100, false)

    // Upsert Operation
    inputDF3.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF3 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF3.count())
    hoodieROViewDF3.select("history", "location", "name", "partition", "timestamp").show(10, false)

    val col41: String =
      s"""{"timestamp": 7.0, "_row_key": "${uuid1}", "name": "p1", "location": 34, "partition": "2015/03/16", "history": []}"""
    val col42: String =
      s"""{"timestamp": 8.0, "_row_key": "${uuid1}", "name": "p1", "location": 33, "partition": "2015/03/16", "history": []}"""
    val col45: String =
      s"""{"timestamp": 9.0, "_row_key": "${uuid1}", "name": "p1", "location": 39, "partition": "2015/03/16", "history": []}"""
    val col46: String =
      s"""{"timestamp": 10.0, "_row_key": "${uuid1}", "name": "p1", "location": 40, "partition": "2015/03/16", "history": []}"""
    val col43: String =
      s"""{"timestamp": 7.0, "_row_key": "${uuid2}", "name": "p2", "location": 35, "partition": "2015/03/16", "history": []}"""
    val col44: String =
      s"""{"timestamp": 8.0, "_row_key": "${uuid2}", "name": "p2", "location": 37, "partition": "2015/03/16", "history": []}"""
    val tb4 = List(col41, col42, col43, col44, col45, col46)
    val inputDF4: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb4, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF4.show(100, false)

    // Upsert Operation
    inputDF4.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime4: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(4, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF4 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF4.count())
    hoodieROViewDF4.select("history", "location", "name", "partition", "timestamp").show(10, false)
  }

  @Test def testCOWHistoryList() {
    val uuid1 = UUID.randomUUID.toString
    val uuid2 = UUID.randomUUID.toString
    val uuid3 = UUID.randomUUID.toString
    // Insert Operation
    val col1: String =
      s"""{"timestamp": 0.0, "_row_key": "${uuid1}", "name": "p1", "location": 1, "partition": "2015/03/16", "history": []}"""
    val col2: String =
      s"""{"timestamp": 1.0, "_row_key": "${uuid1}", "name": "p1", "location": 3, "partition": "2015/03/16", "history": []}"""
    val col3: String =
      s"""{"timestamp": 5.0, "_row_key": "${uuid2}", "name": "p2", "location": 2, "partition": "2015/03/16", "history": []}"""
    //    val col4: String =
    //    s"""{"timestamp": 1.0, "_row_key": "${uuid2}", "name": "p2", "location": 5, "partition": "2015/03/16", "history": []}"""

    val tb = List(col1, col2, col3)
    val inputDF1: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb, 2))
    inputDF1.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY,
        DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    inputDF1.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    hoodieROViewDF1.show()
    assertEquals(2, hoodieROViewDF1.count())

    val col21: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid1}", "name": "p1", "location": 12, "partition": "2015/03/16", "history": []}"""
    val col24: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid1}", "name": "p1", "location": 15, "partition": "2015/03/16", "history": []}"""
    val col25: String =
      s"""{"timestamp": 4.0, "_row_key": "${uuid1}", "name": "p1", "location": 16, "partition": "2015/03/16", "history": []}"""
    val col22: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid2}", "name": "p2", "location": 14, "partition": "2015/03/16", "history": []}"""
    val col23: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid2}", "name": "p2", "location": 11, "partition": "2015/03/16", "history": []}"""
    //    val tb2 = List(col21, col22)
    //    val tb2 = List(col21, col22, col23)
    val tb2 = List(col21, col23, col22, col24, col25)
    val inputDF2: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.show(100, false)

    // Upsert Operation
    inputDF2.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(2, hoodieROViewDF2.count())

    hoodieROViewDF2.select("history", "location", "name", "partition", "timestamp").show(10, false)

    // p1 -> 1, 3, 12, 15
    // p2 -> 2, 5, 11, 14
    //    +----------------------+--------+----+----------+---------+
    //    |history               |location|name|partition |timestamp|
    //    +----------------------+--------+----+----------+---------+
    //    |[-1, 12, 15, 3, -1, 1]|15      |p1  |2015/03/16|3.0      |
    //    |[-1, 14, 11, 5, -1, 2]|11      |p2  |2015/03/16|3.0      |
    //    +----------------------+--------+----+----------+---------+

    //    +------------------+--------+----+----------+---------+
    //    |history           |location|name|partition |timestamp|
    //    +------------------+--------+----+----------+---------+
    //    |[15, 12, 16, 3, 1]|16      |p1  |2015/03/16|4.0      |
    //    |[14, 11, 5, 2]    |11      |p2  |2015/03/16|3.0      |
    //    +------------------+--------+----+----------+---------+

    val col31: String =
      s"""{"timestamp": 50.0, "_row_key": "${uuid1}", "name": "p1", "location": 20, "partition": "2015/03/16", "history": []}"""
    val col32: String =
      s"""{"timestamp": 6.0, "_row_key": "${uuid1}", "name": "p1", "location": 22, "partition": "2015/03/16", "history": []}"""
    val col33: String =
      s"""{"timestamp": 6.0, "_row_key": "${uuid2}", "name": "p2", "location": 23, "partition": "2015/03/16", "history": []}"""
    val col34: String =
      s"""{"timestamp": 45.0, "_row_key": "${uuid3}", "name": "p3", "location": 21, "partition": "2015/03/16", "history": []}"""
    val tb3 = List(col31, col32, col33, col34)
    val inputDF3: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb3, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF3.show(100, false)

    // Upsert Operation
    inputDF3.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF3 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    //    assertEquals(3, hoodieROViewDF3.count())
    hoodieROViewDF3.select("history", "location", "name", "partition", "timestamp").show(10, false)

    val col41: String =
      s"""{"timestamp": 7.0, "_row_key": "${uuid1}", "name": "p1", "location": 34, "partition": "2015/03/16", "history": []}"""
    val col42: String =
      s"""{"timestamp": 8.0, "_row_key": "${uuid3}", "name": "p3", "location": 33, "partition": "2015/03/16", "history": []}"""
    val col45: String =
      s"""{"timestamp": 9.0, "_row_key": "${uuid1}", "name": "p1", "location": 39, "partition": "2015/03/16", "history": []}"""
    val col46: String =
      s"""{"timestamp": 10.0, "_row_key": "${uuid1}", "name": "p1", "location": 40, "partition": "2015/03/16", "history": []}"""
    val col43: String =
      s"""{"timestamp": 7.0, "_row_key": "${uuid2}", "name": "p2", "location": 35, "partition": "2015/03/16", "history": []}"""
    val col44: String =
      s"""{"timestamp": 8.0, "_row_key": "${uuid2}", "name": "p2", "location": 37, "partition": "2015/03/16", "history": []}"""
    val tb4 = List(col41, col42, col43, col44, col45, col46)
    val inputDF4: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb4, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF4.show(100, false)

    // Upsert Operation
    inputDF4.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime4: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(4, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF4 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    assertEquals(3, hoodieROViewDF4.count())
    hoodieROViewDF4.select("history", "location", "name", "partition", "timestamp").show(10, false)
  }


  @Test def testCOWSingleElementList() {
    val uuid1 = UUID.randomUUID.toString
    val uuid2 = UUID.randomUUID.toString
    val uuid3 = UUID.randomUUID.toString

    // Insert Operation
    val col1: String =
      s"""{"timestamp": 0.0, "_row_key": "${uuid1}", "name": "p1", "location": 1, "partition": "2015/03/16", "history": []}"""
    val col3: String =
      s"""{"timestamp": 5.0, "_row_key": "${uuid2}", "name": "p2", "location": 2, "partition": "2015/03/16", "history": []}"""

    val tb = List(col1, col3)
    val inputDF1: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb, 2))
    inputDF1.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY,
        DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    inputDF1.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    hoodieROViewDF1.show()
    //    assertEquals(2, hoodieROViewDF1.count())

    val col21: String =
      s"""{"timestamp": 2.0, "_row_key": "${uuid1}", "name": "p1", "location": 12, "partition": "2015/03/16", "history": []}"""
    val col23: String =
      s"""{"timestamp": 3.0, "_row_key": "${uuid1}", "name": "p1", "location": 11, "partition": "2015/03/16", "history": []}"""
    //    val tb2 = List(col21, col22)
    //    val tb2 = List(col21, col22, col23)
    val tb2 = List(col21, col23)
    val inputDF2: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.show(100, false)

    // Upsert Operation
    inputDF2.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    //    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    //    assertEquals(2, hoodieROViewDF2.count())

    hoodieROViewDF2.select("history", "location", "name", "partition", "timestamp").show(10, false)

    // p1 -> 1, 3, 12, 15
    // p2 -> 2, 5, 11, 14
    //    +----------------------+--------+----+----------+---------+
    //    |history               |location|name|partition |timestamp|
    //    +----------------------+--------+----+----------+---------+
    //    |[-1, 12, 15, 3, -1, 1]|15      |p1  |2015/03/16|3.0      |
    //    |[-1, 14, 11, 5, -1, 2]|11      |p2  |2015/03/16|3.0      |
    //    +----------------------+--------+----+----------+---------+

    //    +------------------+--------+----+----------+---------+
    //    |history           |location|name|partition |timestamp|
    //    +------------------+--------+----+----------+---------+
    //    |[15, 12, 16, 3, 1]|16      |p1  |2015/03/16|4.0      |
    //    |[14, 11, 5, 2]    |11      |p2  |2015/03/16|3.0      |
    //    +------------------+--------+----+----------+---------+

    val col31: String =
      s"""{"timestamp": 50.0, "_row_key": "${uuid1}", "name": "p1", "location": 20, "partition": "2015/03/16", "history": []}"""
    val col33: String =
      s"""{"timestamp": 6.0, "_row_key": "${uuid2}", "name": "p2", "location": 23, "partition": "2015/03/16", "history": []}"""
    val tb3 = List(col31, col33)
    val inputDF3: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb3, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF3.show(100, false)

    // Upsert Operation
    inputDF3.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    //    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF3 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    //    assertEquals(3, hoodieROViewDF3.count())
    hoodieROViewDF3.select("history", "location", "name", "partition", "timestamp").show(10, false)

    val col41: String =
      s"""{"timestamp": 7.0, "_row_key": "${uuid1}", "name": "p1", "location": 34, "partition": "2015/03/16", "history": []}"""
    val col44: String =
      s"""{"timestamp": 8.0, "_row_key": "${uuid2}", "name": "p2", "location": 37, "partition": "2015/03/16", "history": []}"""
    val tb4 = List(col41, col44)
    val inputDF4: Dataset[Row] =
      spark.read.schema(dfSchema).json(spark.sparkContext.parallelize(tb4, 2))
    //    val uniqueKeyCnt = inputDF3.select("_row_key").distinct().count()

    inputDF4.show(100, false)

    // Upsert Operation
    inputDF4.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime4: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(4, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF4 = spark.read
      .format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*");
    //    assertEquals(3, hoodieROViewDF4.count())

    println("HERE")
    hoodieROViewDF4.select("history", "location", "name", "partition", "timestamp").show(10, false)
    //    +-------------------------------------------------+--------+----+----------+---------+
    //    |history                                          |location|name|partition |timestamp|
    //    +-------------------------------------------------+--------+----+----------+---------+
    //    |[50.0#20, 50.0#20, 7.0#34, 2.0#12, 2.0#12, 0.0#1]|34      |p1  |2015/03/16|7.0      |
    //    |[8.0#37, 6.0#23, 6.0#23, 5.0#2, 3.0#11, 3.0#11]  |37      |p2  |2015/03/16|8.0      |
    //    +-------------------------------------------------+--------+----+----------+---------+
  }
}
