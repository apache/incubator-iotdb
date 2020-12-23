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

package org.apache.iotdb.db.query.udf.core.executor;

import java.time.ZoneId;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;

public class UDTFExecutor {

  protected final UDFContext context;
  protected UDTFConfigurations configurations;
  protected UDTF udtf;
  protected ElasticSerializableTVList collector;

  public UDTFExecutor(UDFContext context, ZoneId zoneId) throws QueryProcessException {
    this.context = context;
    configurations = new UDTFConfigurations(zoneId);
    udtf = (UDTF) UDFRegistrationService.getInstance().reflect(context);
    try {
      udtf.beforeStart(new UDFParameters(context.getPaths(), context.getAttributes()),
          configurations);
    } catch (Exception e) {
      throw new QueryProcessException(e.toString());
    }
    configurations.check();
  }

  public void initCollector(long queryId, float collectorMemoryBudgetInMB)
      throws QueryProcessException {
    collector = ElasticSerializableTVList
        .newElasticSerializableTVList(configurations.getOutputDataType(), queryId,
            collectorMemoryBudgetInMB, 1);
  }

  public void execute(Row row) throws QueryProcessException {
    try {
      udtf.transform(row, collector);
    } catch (Exception e) {
      throw new QueryProcessException(e.toString());
    }
  }

  public void execute(RowWindow rowWindow) throws QueryProcessException {
    try {
      udtf.transform(rowWindow, collector);
    } catch (Exception e) {
      throw new QueryProcessException(e.toString());
    }
  }

  public void beforeDestroy() {
    udtf.beforeDestroy();
  }

  public UDFContext getContext() {
    return context;
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }

  public ElasticSerializableTVList getCollector() {
    return collector;
  }
}
