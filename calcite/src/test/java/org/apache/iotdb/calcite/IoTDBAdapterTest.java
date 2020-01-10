package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;
import org.apache.iotdb.calcite.utils.EnvironmentUtils;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IoTDBAdapterTest {

  public static final ImmutableMap<String, String> MODEL =
      ImmutableMap.of("model",
          Sources.of(IoTDBAdapterTest.class.getResource("/model.json"))
              .file().getAbsolutePath());
  private static IoTDB daemon;
  private static String[] sqls = new String[]{

      "SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.other",

      "CREATE timeSERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE timeSERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

      "CREATE timeSERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE timeSERIES root.vehicle.d1.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE timeSERIES root.vehicle.d1.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

      "CREATE timeSERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",

      "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
      "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
      "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
      "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
      "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
      "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
      "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

      "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
      "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
      "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
      "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
      "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
      "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
      "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
      "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
      "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
      "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
      "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",

      "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
      "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
      "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
      "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
      "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
      "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
      "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",

      "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
      "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
      "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
      "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
      "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",

      "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
      "insert into root.vehicle.d0(timestamp,s4) values(100, true)",

      "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
      "insert into root.vehicle.d1(timestamp,s0) values(1000,10)",

      "insert into root.vehicle.d1(timestamp,s1) values(2,9999)",
      "insert into root.vehicle.d1(timestamp,s1) values(1000,5)",

      "insert into root.vehicle.d1(timestamp,s2) values(2,12345.6)",
      "insert into root.vehicle.d1(timestamp,s2) values(2222,2.22)",

      "insert into root.vehicle.d1(timestamp,s3) values(10,'ten')",
      "insert into root.vehicle.d1(timestamp,s3) values(1000,'thousand')",

      "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
      "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

      "insert into root.other.d1(timestamp,s0) values(2, 3.14)",};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSelect() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\"")
        .returnsCount(22)
        .returnsStartingWith(
            "time=1; device=root.vehicle.d0; s0=101; s1=1101; s2=null; s3=null; s4=null")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  /**
   * Test project without time and device columns
   */
  @Test
  public void testProject1() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select \"s0\", \"s2\" from \"root.vehicle\"")
        .limit(1)
        .returns("s0=101; s2=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBProject(s0=[$2], s2=[$4])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  /**
   * Test project with time and device columns
   */
  @Test
  public void testProject2() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select \"time\", \"device\", \"s2\" from \"root.vehicle\"")
        .limit(1)
        .returns("time=2; device=root.vehicle.d0; s2=2.22\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBProject(time=[$0], device=[$1], s2=[$4])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testProjectAlias() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select \"time\" AS \"t\", \"device\" AS \"d\", \"s2\" from \"root.vehicle\"")
        .returnsStartingWith("t=2; d=root.vehicle.d0; s2=2.22")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBProject(t=[$0], d=[$1], s2=[$4])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testLimitOffset() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select \"time\", \"s2\" from \"root.vehicle\" limit 3 offset 2")
        .explainContains("IoTDBLimit(limit=[3], offset=[2])\n")
        .returns("time=3; s2=3.33\n" +
            "time=4; s2=4.44\n" +
            "time=50; s2=null\n");
  }

  @Test
  public void testFilter1() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where \"s0\" <= 10")
        .limit(1)
        .returns("time=1000; device=root.vehicle.d1; s0=10; s1=5; s2=null; s3=thousand; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[<=($2, 10)])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testFilter2() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where \"device\" = 'root.vehicle.d1'")
        .limit(2)
        .returns("time=1; device=root.vehicle.d1; s0=999; s1=null; s2=null; s3=null; s4=null\n"
            + "time=2; device=root.vehicle.d1; s0=null; s1=9999; s2=12345.6; s3=null; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[=($1, 'root.vehicle.d1')])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testFilter3() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where \"time\" < 10 AND \"s0\" >= 150")
        .limit(2)
        .returns("time=2; device=root.vehicle.d0; s0=10000; s1=40000; s2=2.22; s3=null; s4=null\n" +
            "time=1; device=root.vehicle.d1; s0=999; s1=null; s2=null; s3=null; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[AND(<($0, 10), >=($2, 150))])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testFilter4() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where \"device\" = 'root.vehicle.d0' AND \"time\" > 10 AND \"s0\" <= 100")
        .limit(2)
        .returns("time=100; device=root.vehicle.d0; s0=99; s1=199; s2=null; s3=null; s4=true\n" +
            "time=101; device=root.vehicle.d0; s0=99; s1=199; s2=null; s3=ddddd; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[AND(=($1, 'root.vehicle.d0'), >($0, 10), <=($2, 100))])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])\n");
  }

  @Test
  public void testFilter5() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where (\"device\" = 'root.vehicle.d0' AND \"time\" <= 1)" +
            " OR (\"device\" = 'root.vehicle.d1' AND \"s0\" < 100)")
        .limit(2)
        .returns("time=1; device=root.vehicle.d0; s0=101; s1=1101; s2=null; s3=null; s4=null\n" +
            "time=1000; device=root.vehicle.d1; s0=10; s1=5; s2=null; s3=thousand; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[OR(AND(=($1, 'root.vehicle.d0'), <=($0, 1)), " +
            "AND(=($1, 'root.vehicle.d1'), <($2, 100)))])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testFilter6() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where (\"device\" = 'root.vehicle.d0' AND \"time\" <= 1)" +
            " OR (\"device\" = 'root.vehicle.d0' AND \"s0\" < 100)")
        .limit(2)
        .returns("time=1; device=root.vehicle.d0; s0=101; s1=1101; s2=null; s3=null; s4=null\n" +
            "time=100; device=root.vehicle.d0; s0=99; s1=199; s2=null; s3=null; s4=true\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[OR(AND(=($1, 'root.vehicle.d0'), <=($0, 1)), " +
            "AND(=($1, 'root.vehicle.d0'), <($2, 100)))])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

  @Test
  public void testFilter7() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"root.vehicle\" " +
            "where (\"device\" = 'root.vehicle.d0' AND \"time\" <= 1) OR \"s0\" > 100")
        .limit(2)
        .returns("time=1; device=root.vehicle.d0; s0=101; s1=1101; s2=null; s3=null; s4=null\n" +
            "time=1000; device=root.vehicle.d1; s0=10; s1=5; s2=null; s3=thousand; s4=null\n")
        .explainContains("PLAN=IoTDBToEnumerableConverter\n" +
            "  IoTDBFilter(condition=[OR(AND(=($1, 'root.vehicle.d0'), <=($0, 1)), " +
            "AND(=($1, 'root.vehicle.d1'), <($2, 100)))])\n" +
            "    IoTDBTableScan(table=[[IoTDBSchema, root.vehicle]])");
  }

}
