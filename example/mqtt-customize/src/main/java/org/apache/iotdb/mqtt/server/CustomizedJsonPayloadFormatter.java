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
package org.apache.iotdb.mqtt.server;

import org.apache.iotdb.db.mqtt.Message;
import org.apache.iotdb.db.mqtt.PayloadFormatter;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomizedJsonPayloadFormatter implements PayloadFormatter {

  @Override
  public List<Message> format(ByteBuf payload) {
    // Suppose the payload is a json format
    if (payload == null) {
      return null;
    }

    String json = payload.toString(StandardCharsets.UTF_8);

    // parse data from the json and generate Messages and put them into List<Meesage> ret
    List<Message> ret = new ArrayList<>();
    // this is just an example, so we just generate some Messages directly
    for (int i = 0; i < 2; i++) {
      long ts = i;
      Message message = new Message();
      message.setDevice("d" + i);
      message.setTimestamp(ts);
      message.setMeasurements(Arrays.asList("s1", "s2"));
      message.setValues(Arrays.asList("4.0" + i, "5.0" + i));
      ret.add(message);
    }
    return ret;
  }

  @Override
  public String getName() {
    // set the value of mqtt_payload_formatter in iotdb-engine.properties as the following string:
    return "CustomizedJson";
  }
}
