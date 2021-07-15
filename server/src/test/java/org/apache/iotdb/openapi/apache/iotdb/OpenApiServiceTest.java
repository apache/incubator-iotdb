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
package org.apache.iotdb.openapi.apache.iotdb;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.openapi.gen.handler.NotFoundException;
import org.apache.iotdb.openapi.gen.handler.filter.BasicSecurityContext;
import org.apache.iotdb.openapi.gen.handler.impl.V1ApiServiceImpl;
import org.apache.iotdb.openapi.gen.handler.model.User;
import org.apache.iotdb.openapi.gen.model.GroupByFillPlan;
import org.apache.iotdb.openapi.gen.model.ReadData;
import org.apache.iotdb.openapi.gen.model.WriteData;

import com.google.gson.Gson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OpenApiServiceTest {
  GroupByFillPlan groupByFillPlan = new GroupByFillPlan();
  User user = new User();
  BasicSecurityContext basicSecurityContext = new BasicSecurityContext(user, false);
  V1ApiServiceImpl v1ApiService = new V1ApiServiceImpl();

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    user.setUsername("root");
    user.setPassword("root");
    BigDecimal stime = BigDecimal.valueOf(1);
    BigDecimal etime = BigDecimal.valueOf(10);
    List<String> path = new ArrayList<String>();
    path.add("root");
    path.add("sg");
    path.add("aa");
    groupByFillPlan.setStime(stime);
    groupByFillPlan.setEtime(etime);
    groupByFillPlan.setPaths(path);
    // groupByFillPlan.set("1ms");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void postV1GrafanaData() throws NotFoundException {
    Response response = v1ApiService.postV1GrafanaData(groupByFillPlan, basicSecurityContext);
    assertNotNull(response.getEntity());
  }

  @Test
  public void insertDb() {
    String sql = "insert into root.sg(time,aa) values(3,3)";
    String result = v1ApiService.insertDb(basicSecurityContext, sql);
    assertEquals("success", result);
  }

  @Test
  public void postV1RestDataRead() throws NotFoundException {
    ReadData readData = new ReadData();
    readData.setSql("select * from root.sg");
    Response response = v1ApiService.postV1RestDataRead(readData, basicSecurityContext);
    assertNotNull(response);
    Gson json = new Gson();
    System.out.println(response.getEntity().toString());
    assertNotNull(json.fromJson(response.getEntity().toString(), List.class).size());
  }

  @Test
  public void postV1NonQuery() throws NotFoundException {
    ReadData readData = new ReadData();
    readData.setSql("delete from root.sg");
    Response result = v1ApiService.postV1NonQuery(readData, basicSecurityContext);
    Gson json = new Gson();
    assertEquals(
        "execute sucessfully",
        json.fromJson(result.getEntity().toString(), Map.class).get("message"));
  }

  @Test
  public void postV1RestDataWrite() throws NotFoundException {
    WriteData writeData = new WriteData();
    List<String> path = new ArrayList<String>();
    path.add("root");
    path.add("sg");
    List<String> params = new ArrayList<String>();
    params.add("time");
    params.add("a1");
    List<String> value = new ArrayList<String>();
    value.add("1");
    value.add("2");
    writeData.setPaths(path);
    writeData.setParams(params);
    writeData.setValuses(value);
    Response result = v1ApiService.postV1RestDataWrite(writeData, basicSecurityContext);
    Gson json = new Gson();
    assertEquals(
        "write data success",
        json.fromJson(result.getEntity().toString(), Map.class).get("message"));
  }

  @Test
  public void postV1GrafanaNode() throws NotFoundException {
    List<String> requestBody = new ArrayList<String>();
    requestBody.add("root");
    requestBody.add("sg");
    Response result = v1ApiService.postV1GrafanaNode(requestBody, basicSecurityContext);
    assertNotNull(result.getEntity());
  }

  @Test
  public void postV1GrafanaDataSimplejson() throws NotFoundException {
    Response response =
        v1ApiService.postV1GrafanaDataSimplejson(groupByFillPlan, basicSecurityContext);
    assertNotNull(response.getEntity());
  }
}
