/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.spark.tsfile

import java.io.{ByteArrayOutputStream, File}
import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iotdb.hadoop.fileSystem.HDFSInput
import org.apache.iotdb.spark.constant.TestConstant
import org.apache.iotdb.spark.tool.TsFileWriteTool
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.read.{TsFileSequenceReader, common}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val baseFolder = TestConstant.BASE_OUTPUT_PATH.concat("suitTestBaseDir")
  private val tsfileFolder1 = baseFolder + "/tsfileFolder1"
  private val tsfile1 = tsfileFolder1 + "/test1.tsfile"
  private val tsfile2 = tsfileFolder1 + "/test2.tsfile"

  private val tsfileFolder2 = baseFolder + "/tsfileFolder2"
  private val tsfile3 = tsfileFolder2 + "/test.tsfile"

  private val tsfileFolder3 = baseFolder + "/tsfileFolder3"
  private val tsfile4 = tsfileFolder3 + "/test.tsfile"

  private val outputPath = baseFolder + "/output"
  private val outputPathFile = outputPath + "/part-m-00000"

  private val outputPath2 = baseFolder + "/output2"
  private val outputPathFile2 = outputPath2 + "/part-m-00000"

  private val outputPath3 = baseFolder + "/output3"
  private val outputPathFile3 = outputPath3

  private val outputHDFSPath = "hdfs://localhost:9000/usr/hadoop/output"
  private val outputHDFSPathFile = outputHDFSPath + "/part-m-00000"

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val base = new File(baseFolder)
    if (!base.exists())
      base.mkdirs()

    val tsfile_folder1 = new File(tsfileFolder1)
    if (tsfile_folder1.exists()) {
      deleteDir(tsfile_folder1)
    }
    tsfile_folder1.mkdirs()

    val tsfile_folder2 = new File(tsfileFolder2)
    if (tsfile_folder2.exists()) {
      deleteDir(tsfile_folder2)
    }
    tsfile_folder2.mkdirs()

    val tsfile_folder3 = new File(tsfileFolder3)
    if (tsfile_folder3.exists()) {
      deleteDir(tsfile_folder3)
    }
    tsfile_folder3.mkdirs()

    new TsFileWriteTool().create1(tsfile1)
    new TsFileWriteTool().create2(tsfile2)
    new TsFileWriteTool().create3(tsfile3)
    new TsFileWriteTool().create4(tsfile4)

    val output = new File(outputPath)
    if (output.exists())
      deleteDir(output)
    val output2 = new File(outputPath2)
    if (output2.exists())
      deleteDir(output2)


    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    val folder1 = new File(tsfileFolder1)
    deleteDir(folder1)
    val folder2 = new File(tsfileFolder2)
    deleteDir(folder2)
    val folder3 = new File(tsfileFolder3)
    deleteDir(folder3)

    val out = new File(outputPath)
    deleteDir(out)
    val out2 = new File(outputPath2)
    deleteDir(out2)

    val base = new File(baseFolder)
    deleteDir(base)

    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  def deleteDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.list().foreach(f => {
        deleteDir(new File(dir, f))
      })
    }
    dir.delete()
  }

  test("test write 1") {
    val df = spark.read.tsfile(tsfile1)
    df.write.tsfile(outputPath)
    val newDf = spark.read.tsfile(outputPathFile)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write 2") {
    val df = spark.read.tsfile(tsfile2)
    df.write.tsfile(outputPath2)
    val newDf = spark.read.tsfile(outputPathFile2)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("test write 3") {
    val df = spark.read.tsfile(tsfile3, isNarrowForm = true)
    df.write.tsfile(outputPath3, isNarrowForm = true)
    val newDf = spark.read.tsfile(outputPathFile3, isNarrowForm = true)
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }

  test("testSelect * from tsfile1") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(7, count)
  }

  test("testSelect * from tsfile2") {
    val df = spark.read.tsfile(tsfile2)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(TsFileWriteTool.largeNum, count)
  }

  test("testSelect * from tsfile2 in part") {
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 256)

    val df = spark.read.tsfile(tsfile2)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(TsFileWriteTool.largeNum, count)

    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 128)
  }

  test("testCount") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select count(*) from tsfile_table")
    Assert.assertEquals(7, newDf.head().apply(0).asInstanceOf[Long])
  }

  test("testSelect time") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time from tsfile_table")
    Assert.assertEquals(7, newDf.count())
  }

  test("testSelectString") {
    val df = spark.read.tsfile(tsfile3)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "`device_1.sensor_2` = \"Monday\"")
    Assert.assertEquals(1, newDf.count())
  }

  test("testSelectBoolean") {
    val df = spark.read.tsfile(tsfile3)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "`device_1.sensor_1` = true")
    Assert.assertEquals(2, newDf.count())
  }

  test("testSelectWithFilterAnd") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "`device_1.sensor_1`>0 and `device_1.sensor_2` < 22")
    Assert.assertEquals(5, newDf.count())
  }

  test("testSelectWithFilterOr") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "`device_1.sensor_1`>0 or `device_1.sensor_2` < 22")
    Assert.assertEquals(7, newDf.count())
  }

  test("testSelectComplex") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "(`device_1.sensor_1`>0 or `device_1.sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testMultiFiles") {
    val df = spark.read.tsfile(tsfileFolder1)
    df.createOrReplaceTempView("tsfile_table")
    Assert.assertEquals(TsFileWriteTool.largeNum + 7, df.count())
  }

  test("testMultiFilesWithFilter1") {
    val df = spark.read.tsfile(tsfileFolder1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1` >0 " +
      "and `device_1.sensor_1` <10 or `device_1.sensor_2` >0")
    Assert.assertEquals(16, newDf.count())
  }

  test("testQuerySchema") {
    val df = spark.read.format("org.apache.iotdb.spark.tsfile").load(tsfile1)

    val expected = StructType(Seq(
      StructField(QueryConstant.RESERVED_TIME, LongType, nullable = true),
      StructField("device_1.sensor_3", IntegerType, nullable = true),
      StructField("device_1.sensor_1", FloatType, nullable = true),
      StructField("device_1.sensor_2", IntegerType, nullable = true),
      StructField("device_2.sensor_3", IntegerType, nullable = true),
      StructField("device_2.sensor_1", FloatType, nullable = true),
      StructField("device_2.sensor_2", IntegerType, nullable = true)
    ))
    Assert.assertEquals(expected, df.schema)
  }

  test("testTransform1") {
    val df = spark.read.tsfile(tsfile1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where (`device_1.sensor_1`>0 or `device_1.sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testTransform2") {
    val df = spark.read.tsfile(tsfile1, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where `device_name` = 'device_1' and (`sensor_1`>0 or `sensor_2` < 22) and time < 4")
    Assert.assertEquals(3, newDf.count())
  }

  test("testTransform3") {
    val df = spark.read.tsfile(tsfile1, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table " +
      "where (`sensor_1`>0 or `sensor_2` < 22) and time < 4")
    Assert.assertEquals(5, newDf.count())
  }

  /*
device_1: 400000 rows, time range [0,399999], interval 1
device_2: 400000 rows, time range [0,799998], interval 2
*/
  test("partition test: narrow table, no filter") {
    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 128)

    val df = spark.read.tsfile(tsfile4, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    Assert.assertEquals(800000, newDf.count())

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  /*
    device_1 & device_2: 400000 rows, time range [0,399999], interval 1
    device_2: 200000 rows, time range [400000,799998], interval 2
   */
  test("partition test: wide table, no filter") {
    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 128)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    Assert.assertEquals(600000, newDf.count())

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  /*
    sql: select * from tsfile_table where time > 131040 and time < 131050:
    involves device_1's chunkGroup1&2 and device_2's chunkGroup1.

    maxPartitionBytes size makes device_1's chunkGroup1 falls into the first partition, and
    device_2's chunkGroup1, device_1's chunkGroup2 fall into the following several partitions.

    In this test, note that the results from device_1's two chunkgroups are NOT presented together
    because they are retrieved in different tasks.
   */
  test("partition test: narrow table, global time filter, small partition size") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(1).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup - 100)

    val df = spark.read.tsfile(tsfile4, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "time > 131040 and time < 131050")

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      newDf.show(newDf.count.toInt, false)
    }
    val actual = outCapture.toByteArray.map(_.toChar)

    val expect =
        "+------+-----------+--------+--------+--------+\n" +
        "|time  |device_name|sensor_3|sensor_1|sensor_2|\n" +
        "+------+-----------+--------+--------+--------+\n" +
        "|131042|device_2   |true    |131042  |131042.0|\n" +
        "|131044|device_2   |true    |131044  |131044.0|\n" +
        "|131046|device_2   |true    |131046  |131046.0|\n" +
        "|131048|device_2   |true    |131048  |131048.0|\n" +
        "|131041|device_1   |null    |131041  |null    |\n" +
        "|131042|device_1   |null    |131042  |null    |\n" +
        "|131043|device_1   |null    |131043  |null    |\n" +
        "|131044|device_1   |null    |131044  |null    |\n" +
        "|131045|device_1   |null    |131045  |null    |\n" +
        "|131046|device_1   |null    |131046  |null    |\n" +
        "|131047|device_1   |null    |131047  |null    |\n" +
        "|131048|device_1   |null    |131048  |null    |\n" +
        "|131049|device_1   |null    |131049  |null    |\n" +
        "+------+-----------+--------+--------+--------+"

    println("???" + util.Arrays.toString(actual))
    Assert.assertArrayEquals(expect.toCharArray, actual.dropRight(2))

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }


  /*
    sql: select * from tsfile_table where time > 131040 and time < 131050:
    involves device_1's chunkGroup1&2 and device_2's chunkGroup1.

    maxPartitionBytes size makes device_1's chunkGroup1, device_2's chunkGroup1,
    device_1's chunkGroup2 fall into the first partition.

    Thus, compared to the former test("global time filter test 1"), this test has two differences:
    1) the results from device_1's two chunkgroups are presented together as they are retrieved in
       the same task.
    2) device_2's data is presented first as the dataset at the end of the list is retrieved first
       by code.
   */
  test("partition test: narrow table, global time filter, larger partition size") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "time > 131040 and time < 131050")

    newDf.show()

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      newDf.show(newDf.count.toInt, false)
    }
    val actual = outCapture.toByteArray.map(_.toChar)

    val expect =
      "+------+-----------+--------+--------+--------+\n" +
        "|time  |device_name|sensor_3|sensor_1|sensor_2|\n" +
        "+------+-----------+--------+--------+--------+\n" +
        "|131042|device_2   |true    |131042  |131042.0|\n" +
        "|131044|device_2   |true    |131044  |131044.0|\n" +
        "|131046|device_2   |true    |131046  |131046.0|\n" +
        "|131048|device_2   |true    |131048  |131048.0|\n" +
        "|131041|device_1   |null    |131041  |null    |\n" +
        "|131042|device_1   |null    |131042  |null    |\n" +
        "|131043|device_1   |null    |131043  |null    |\n" +
        "|131044|device_1   |null    |131044  |null    |\n" +
        "|131045|device_1   |null    |131045  |null    |\n" +
        "|131046|device_1   |null    |131046  |null    |\n" +
        "|131047|device_1   |null    |131047  |null    |\n" +
        "|131048|device_1   |null    |131048  |null    |\n" +
        "|131049|device_1   |null    |131049  |null    |\n" +
        "+------+-----------+--------+--------+--------+"

    Assert.assertArrayEquals(expect.toCharArray, actual.dropRight(2))

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, global time filter 1") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_1.sensor_1` from tsfile_table where " +
      "time > 131040 and time < 131050")

    Assert.assertEquals(9, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, global time filter 2") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_2.sensor_1` from tsfile_table where " +
      "time > 131040 and time < 131050")

    newDf.show()
    Assert.assertEquals(4, newDf.rdd.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, global time filter 3") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where " +
      "time > 131040 and time < 131050")

    Assert.assertEquals(9, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: narrow table, value filter") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, true)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time from tsfile_table where " +
      "`device_name` = 'device_2' and  `sensor_1` > 262080 and `sensor_2` <= 600000")

    Assert.assertEquals(168960, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, value filter 1") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_2.sensor_3` from tsfile_table where " +
      "`device_2.sensor_1` > 262080 and `device_2.sensor_2` <= 600000")

    Assert.assertEquals(168960, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, value filter 2") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_2.sensor_3` from tsfile_table where " +
      "`device_2.sensor_1` > 262080 and `device_2.sensor_2` <= 600000 and " +
      "`device_1.sensor_1` < 400000")

    Assert.assertEquals(68959, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, value filter 3") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_1.sensor_1` from tsfile_table where " +
      "`device_2.sensor_1` > 262080 and `device_2.sensor_2` <= 600000 and " +
      "`device_1.sensor_1` < 400000")

    Assert.assertEquals(68959, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

  test("partition test: wide table, value filter 4") {
    var conf: Configuration = spark.sparkContext.hadoopConfiguration
    val in = new HDFSInput(new Path(new URI(tsfile4)), conf)
    val reader: TsFileSequenceReader = new TsFileSequenceReader(in)
    val tsFileMetaData = reader.readFileMetadata
    val chunkMetadataList = reader.getChunkMetadataList(new common.Path("device_1", "sensor_1"), true)
    val endOffsetOfChunkGroup = chunkMetadataList.get(2).getOffsetOfChunkHeader

    val tmp = spark.conf.get("spark.sql.files.maxPartitionBytes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", endOffsetOfChunkGroup + 100)

    val df = spark.read.tsfile(tsfile4, false)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select time,`device_2.sensor_1` from tsfile_table where " +
      "`device_1.sensor_1` < 400000")

    Assert.assertEquals(400000, newDf.count())

    reader.close() // DO NOT FORGET THIS

    spark.conf.set("spark.sql.files.maxPartitionBytes", tmp)
  }

}