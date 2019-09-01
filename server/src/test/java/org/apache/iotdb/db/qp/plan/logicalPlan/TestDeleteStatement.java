package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDeleteStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void delete1() throws LogicalOperatorException {
    RootOperator op = generator.getLogicalPlan("delete from root.d1.s1 where time < 2016-11-16 16:22:33+08:00");
    assertEquals(SQLConstant.TOK_DELETE, op.getTokenIntType());
    assertEquals(DeleteOperator.OperatorType.DELETE, ((DeleteOperator) op).getType());
    Path expectedPath = new Path("root.d1.s1");
    assertEquals(1, ((DeleteOperator) op).getSelectedPaths().size());
    assertEquals(expectedPath, ((DeleteOperator) op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.LESSTHAN, ((DeleteOperator) op).getFilterOperator().getTokenIntType());
    assertEquals(new Path("time"), ((DeleteOperator) op).getFilterOperator().getSinglePath());
    assertEquals(parseTimeFormat("2016-11-16 16:22:33+08:00") - 1, ((DeleteOperator) op).getTime());
  }

  @Test
  public void delete2() throws LogicalOperatorException {
    RootOperator op = generator.getLogicalPlan("delete from root.d1.s1 where time < now();");
    assertEquals(SQLConstant.TOK_DELETE, op.getTokenIntType());
    assertEquals(DeleteOperator.OperatorType.DELETE, ((DeleteOperator) op).getType());
    Path expectedPath = new Path("root.d1.s1");
    assertEquals(SQLConstant.LESSTHAN, ((DeleteOperator) op).getFilterOperator().getTokenIntType());
    assertEquals(new Path("time"), ((DeleteOperator) op).getFilterOperator().getSinglePath());
    assertTrue(System.currentTimeMillis() - ((DeleteOperator) op).getTime() < 10);
  }

  @Test
  public void delete3() {
    RootOperator op = generator.getLogicalPlan("delete from root.d1.s1 where time < 12345678909876");
    assertEquals(SQLConstant.TOK_DELETE, op.getTokenIntType());
    assertEquals(DeleteOperator.OperatorType.DELETE, ((DeleteOperator) op).getType());
    Path expectedPath = new Path("root.d1.s1");
    assertEquals(1, ((DeleteOperator) op).getSelectedPaths().size());
    assertEquals(expectedPath, ((DeleteOperator) op).getSelectedPaths().get(0));
    assertEquals(SQLConstant.LESSTHAN, ((DeleteOperator) op).getFilterOperator().getTokenIntType());
    assertEquals(new Path("time"), ((DeleteOperator) op).getFilterOperator().getSinglePath());
    assertEquals(12345678909876L - 1, ((DeleteOperator) op).getTime());
  }

  @Test
  public void delete4() {
    RootOperator op = generator.getLogicalPlan("delete from root.d1.s1,root.d2.s3 where time < now();");
    assertEquals(SQLConstant.TOK_DELETE, op.getTokenIntType());
    assertEquals(DeleteOperator.OperatorType.DELETE, ((DeleteOperator) op).getType());
    Path expectedPath1 = new Path("root.d1.s1"), expectedPath2 = new Path("root.d2.s3");
    assertEquals(2, ((DeleteOperator) op).getSelectedPaths().size());
    assertEquals(expectedPath1, ((DeleteOperator) op).getSelectedPaths().get(0));
    assertEquals(expectedPath2, ((DeleteOperator) op).getSelectedPaths().get(1));
    assertEquals(SQLConstant.LESSTHAN, ((DeleteOperator) op).getFilterOperator().getTokenIntType());
    assertEquals(new Path("time"), ((DeleteOperator) op).getFilterOperator().getSinglePath());
    assertTrue(System.currentTimeMillis() - ((DeleteOperator) op).getTime() < 10);
  }

  @Test(expected = SqlParseException.class)
  public void delete5() {
    RootOperator op = generator.getLogicalPlan("delete from root.d1.*,root.*.s2 where !(time < 123456)");
  }

  private long parseTimeFormat(String timestampStr) throws LogicalOperatorException {
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    return DatetimeUtils.convertDatetimeStrToLong(timestampStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
  }
}
