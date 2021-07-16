<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Customized IoTDB-MQTT-Broker Example

## Function
```
The example is to show how to customize your MQTT message format
```

## Usage

* Define your implementation which implements `PayloadFormatter.java`
* modify the file in `src/main/resources/META-INF/services/org.apache.iotdb.db.mqtt.PayloadFormatter`:
  clean the file and put your implementation class name into the file
* compile your implementation as a jar file

  
Then, in your server: 
* put the jar into your server's lib folder
* Update configuration to enable MQTT service. (`enable_mqtt_service=true` in iotdb-engine.properties)
* et the value of `mqtt_payload_formatter` in `conf/iotdb-engine.properties` as the value of getName() in your implementation 
* Launch the IoTDB server.
* Now IoTDB will use your implementation to parse the MQTT message.

