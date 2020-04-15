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
package org.apache.iotdb.web.grafana.dao.impl;

import java.time.Duration;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.dao.BasicDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by dell on 2017/7/17.
 */
@Repository
public class BasicDaoImpl implements BasicDao {

  private static final Logger logger = LoggerFactory.getLogger(BasicDaoImpl.class);

  private static final String CONFIG_PROPERTY_FILE = "application.properties";

  private final JdbcTemplate jdbcTemplate;

  private static long TIMESTAMP_RADIX = 1L;

  @Autowired
  public BasicDaoImpl(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
    try (InputStream inputStream = new FileInputStream(new File(CONFIG_PROPERTY_FILE))) {
      Properties properties = new Properties();
      properties.load(inputStream);
      String tsPrecision = properties.getProperty("timestamp_precision", "ms");
      switch (tsPrecision) {
        case "us":
          TIMESTAMP_RADIX = 1000;
          break;
        case "ns":
          TIMESTAMP_RADIX = 1000_000;
          break;
        default:
          TIMESTAMP_RADIX = 1;
      }
      logger.info("Use timestamp precision {}", tsPrecision);
    } catch (IOException e) {
      logger.error("Can not find properties [timestamp_precision], use default value [ms]");
      TIMESTAMP_RADIX = 1;
    }
  }

  @Override
  public List<String> getMetaData() {
    ConnectionCallback<Object> connectionCallback = new ConnectionCallback<Object>() {
      public Object doInConnection(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("show timeseries root.*");
        ResultSet resultSet = statement.getResultSet();
        logger.info("Start to get timeseries");
        List<String> columnsName = new ArrayList<>();
        while (resultSet.next()) {
          String timeseries = resultSet.getString(1);
          columnsName.add(timeseries.substring(5));
        }
        return columnsName;
      }
    };
    return (List<String>) jdbcTemplate.execute(connectionCallback);
  }

  @Override
  public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    Long from = zonedCovertToLong(timeRange.left);
    Long to = zonedCovertToLong(timeRange.right);
    final long hours = Duration.between(timeRange.left, timeRange.right).toHours();

    Properties properties = new Properties();
    String interval = properties.getProperty("interval", "1m");

    List<TimeValues> rows = null;
    String sql = String.format("SELECT %s FROM root.%s WHERE time > %d and time < %d",
        s.substring(s.lastIndexOf('.') + 1), s.substring(0, s.lastIndexOf('.')),
        from * TIMESTAMP_RADIX, to * TIMESTAMP_RADIX);
    String columnName = "root." + s;
    if (hours > 5) {
      if (hours < 30 * 24 && hours > 24) {
        interval = "1d";
      } else {
        interval = "1h";
      }
      sql = String.format(
          "SELECT avg(%s) FROM root.%s WHERE time > %d and time < %d group by ([%d, %d),%s)",
          s.substring(s.lastIndexOf('.') + 1), s.substring(0, s.lastIndexOf('.')), from, to, from,
          to, interval);
      columnName = "avg(root." + s + ")";
    }
    logger.info(sql);
    try {
      rows = jdbcTemplate.query(sql, new TimeValuesRowMapper(columnName));
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return rows;
  }

  private Long zonedCovertToLong(ZonedDateTime time) {
    return time.toInstant().toEpochMilli();
  }

  static class TimeValuesRowMapper implements RowMapper<TimeValues> {

    String columnName;

    TimeValuesRowMapper(String columnName) {
      this.columnName = columnName;
    }

    @Override
    public TimeValues mapRow(ResultSet resultSet, int i) throws SQLException {
      TimeValues tv = new TimeValues();
      tv.setTime(resultSet.getLong("Time") / TIMESTAMP_RADIX);
      String valueString = resultSet.getString(columnName);
      if (valueString != null) {
        try {
          tv.setValue(Float.parseFloat(resultSet.getString(columnName)));
        } catch (Exception e) {
          tv.setValue(resultSet.getString(columnName));
        }
      }
      return tv;
    }
  }

}
