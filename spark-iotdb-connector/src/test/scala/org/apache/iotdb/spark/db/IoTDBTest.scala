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

package org.apache.iotdb.spark.db

import java.io.ByteArrayOutputStream

import org.apache.iotdb.db.conf.IoTDBConstant
import org.apache.iotdb.db.service.IoTDB
import org.apache.iotdb.jdbc.Config
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class IoTDBTest extends FunSuite with BeforeAndAfterAll {
  private var daemon: IoTDB = _

  private val testFile = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val csvPath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.csv"
  private val tsfilePath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/test.tsfile"
  private val errorPath: java.lang.String = "/home/hadoop/git/tsfile/delta-spark/src/test/resources/errortest.tsfile"
  private var sqlContext: SQLContext = _
  private var spark: SparkSession = _

  @Before
  override protected def beforeAll(): Unit = {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "/Users/heimingz/IoTDB/my_iotdb/spark-iotdb-connector/target/test-classes")
    super.beforeAll()

    EnvironmentUtils.closeStatMonitor()
    daemon = IoTDB.getInstance
    daemon.active()
    EnvironmentUtils.envSetUp()
    Class.forName(Config.JDBC_DRIVER_NAME)
    EnvironmentUtils.prepareData()

    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  @AfterClass
  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.stop()
    }

    daemon.stop()
    EnvironmentUtils.cleanEnv()

    super.afterAll()
  }

  test("test show data") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root").load
    Assert.assertEquals(7505, df.count())
  }

  test("test show data with partition") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root")
      .option("lowerBound", 1).option("upperBound", System.nanoTime() / 1000 / 1000)
      .option("numPartition", 10).load
    Assert.assertEquals(7505, df.count())
  }

  test("test filter data") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root where time < 2000 and time > 1000").load

    Assert.assertEquals(499, df.count())
  }

  test("test filter data with partition") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root where time < 2000 and time > 1000")
      .option("lowerBound", 1)
      .option("upperBound", 10000).option("numPartition", 10).load

    Assert.assertEquals(499, df.count())
  }

  test("test transform to narrow") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root where time < 1100 and time > 1000").load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    Assert.assertEquals(198, narrow_df.count())
  }

  test("test transform to narrow with partition") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root where time < 1100 and time > 1000")
      .option("lowerBound", 1).option("upperBound", 10000)
      .option("numPartition", 10).load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    Assert.assertEquals(198, narrow_df.count())
  }

  test("test transform back to wide") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select * from root where time < 1100 and time > 1000").load
    val narrow_df = Transformer.toNarrowForm(spark, df)
    val wide_df = Transformer.toWideForm(spark, narrow_df)
    Assert.assertEquals(99, wide_df.count())
  }

  test("test aggregate sql") {
    val df = spark.read.format("org.apache.iotdb.sparkdb")
      .option("url", "jdbc:iotdb://127.0.0.1:6667/")
      .option("sql", "select count(d0.s0),count(d0.s1) from root.vehicle").load

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(df.count.toInt, false)
    }
    val actual = outCapture.toByteArray.map(_.toChar)

    val expect =
      "+-------------------------+-------------------------+\n" +
        "|count(root.vehicle.d0.s0)|count(root.vehicle.d0.s1)|\n" +
        "+-------------------------+-------------------------+\n" +
        "|7500                     |7500                     |\n" +
        "+-------------------------+-------------------------+\n"

    Assert.assertArrayEquals(expect.toCharArray, actual.dropRight(2))
  }
}

