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

## MQTT协议

[MQTT](http://mqtt.org/)是机器对机器（M2M）/“物联网”连接协议。

它被设计为一种非常轻量级的发布/订阅消息传递。

对于与需要较小代码占用和/或网络带宽非常宝贵的远程位置的连接很有用。

IoTDB支持MQTT v3.1（OASIS标准）协议。
IoTDB服务器包括内置的MQTT服务，该服务允许远程设备将消息直接发送到IoTDB服务器。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/6711230/78357432-0c71cf80-75e4-11ea-98aa-c43a54d469ce.png">


### 内置MQTT服务
内置的MQTT服务提供了通过MQTT直接连接到IoTDB的能力。 它侦听来自MQTT客户端的发布消息，然后立即将数据写入存储。
MQTT主题与IoTDB时间序列相对应。
消息有效载荷可以由Java SPI加载的`PayloadFormatter`格式化为事件，默认实现为`JSONPayloadFormatter` 
   默认的`json`格式化程序支持两种json格式，以下是MQTT消息有效负载示例：

```json
 {
      "device":"root.sg.d1",
      "timestamp":1586076045524,
      "measurements":["s1","s2"],
      "values":[0.530635,0.530635]
 }
```
或者
```json
{
      "device":"root.sg.d1",
      "timestamps":[1586076045524,1586076065526],
      "measurements":["s1","s2"],
      "values":[[0.530635,0.530635], [0.530655,0.530695]]
  }
```

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/6711230/78357469-1bf11880-75e4-11ea-978f-a53996667a0d.png">

### MQTT配置
默认情况下，IoTDB MQTT服务从`${IOTDB_HOME}/${IOTDB_CONF}/iotdbengine.properties`加载配置。

配置如下：

| 名称      | 描述         | 默认 |
| ------------- |:-------------:|:------:|
| enable_mqtt_service      | 是否启用mqtt服务 | false |
| mqtt_host      | mqtt服务绑定主机 | 0.0.0.0 |
| mqtt_port      | mqtt服务绑定端口 |   1883 |
| mqtt_handler_pool_size | 处理mqtt消息的处理程序池大小 |    1 |
| mqtt_payload_formatter | mqtt消息有效负载格式化程序 |    json |
| mqtt_max_message_size | mqtt消息最大长度（字节）|   1048576 |

### 示例代码
以下是mqtt客户端将消息发送到IoTDB服务器的示例。

 ```java
MQTT mqtt = new MQTT();
mqtt.setHost("127.0.0.1", 1883);
mqtt.setUserName("root");
mqtt.setPassword("root");

BlockingConnection connection = mqtt.blockingConnection();
connection.connect();

Random random = new Random();
for (int i = 0; i < 10; i++) {
    String payload = String.format("{\n" +
            "\"device\":\"root.sg.d1\",\n" +
            "\"timestamp\":%d,\n" +
            "\"measurements\":[\"s1\"],\n" +
            "\"values\":[%f]\n" +
            "}", System.currentTimeMillis(), random.nextDouble());

    connection.publish("root.sg.d1.s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
}

connection.disconnect();
 ```


### 自定义MQTT消息格式

事实上可以通过简单编程来实现MQTT消息的格式自定义。
可以在源码的`example/mqtt-customize`项目中找到一个简单示例。

步骤:
* 创建一个Java项目，增加如下依赖
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-server</artifactId>
            <version>${project.version}</version>
        </dependency>
```
* 创建一个实现类，实现接口 `org.apache.iotdb.db.mqtt.PayloadFormatter.java`
```java
package org.apache.iotdb.mqtt.server;

import io.netty.buffer.ByteBuf;
import org.apache.iotdb.db.mqtt.Message;
import org.apache.iotdb.db.mqtt.PayloadFormatter;

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
```
* 修改项目中的 `src/main/resources/META-INF/services/org.apache.iotdb.db.mqtt.PayloadFormatter`文件:
  将示例中的文件内容清除，并将刚才的实现类的全名（包名.类名）写入文件中。注意，这个文件中只有一行。
  在本例中，文件内容为: `org.apache.iotdb.mqtt.server.CustomizedJsonPayloadFormatter`
* 编译项目生成一个jar包: `mvn package -DskipTests`


在IoTDB服务端:
* 将刚才的jar包放入IoTDB的lib文件夹内
* 打开MQTT服务参数. (`enable_mqtt_service=true` in `conf/iotdb-engine.properties`)
* 用刚才的实现类中的getName() 方法的返回值 设置为 `conf/iotdb-engine.properties`中`mqtt_payload_formatter`的值， 
  , 在本例中，为 `CustomizedJson`
* 启动IoTDB
* 搞定.

More: MQTT协议的消息不限于json，你还可以用任意二进制。通过如下函数获得：
`payload.forEachByte()` or `payload.array`。 