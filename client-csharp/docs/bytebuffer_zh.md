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

## IoTDB CSharp Client ByteBuffer实现介绍

在进行`RowRecords`以及`Tablet`的插入时，我们需要对多行RowRecord和Tablet进行序列化以进行发送。客户端中的序列化实现主要依赖于ByteBuffer完成。接下来我们介绍ByteBuffer的实现细节。本文包含如下几点内容:
 - 序列化的协议
 - C#与Java的大小端的差异
 - ByteBuffer内存倍增算法

### 一、序列化协议
客户端向IoTDB服务器发送的序列化数据总体应该包含两个信息。
 -  数据类型
 -  数据本身

其中对于`字符串`的序列化时，我们需要再加入字符串的长度信息。即一个字符串的序列化完整结果为:

    [类型][长度][数据内容]
接下来我们分别介绍`RowRecord`、`Tablet`的序列化方式

#### 1.1 RowRecord
我们对RowRecord进行序列化时，`伪代码`如下:

        public byte[] value_to_bytes(List<TSDataType> data_types, List<string> values){
            ByteBuffer buffer = new ByteBuffer(values.Count);
            for(int i = 0;i < data_types.Count(); i++){
                buffer.add_type((data_types[i]);
                buffer.add_val(values[i]);
            }
        }

对于其序列化的结果格式如下:

    [数据类型1][数据1][数据类型2][数据2]...[数据类型N][数据N]
 其中数据类型为自定义的`Enum`变量，分别如下:

    public enum TSDataType{BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NONE};

#### 1.2. Tablet序列化
使用`Tabelt`进行数据插入时有如下限制:

    限制：Tablet中数据不能有空值
由于向 `IoTDB`服务器发送`Tablet`数据插入请求时会携带`行数`， `列数`, `列数据类型`，所以`Tabelt`序列化时我们不需要加入数据类型信息。`Tablet`是`按照列进行序列化`，这是因为后端可以通过行数得知出当前列的元素个数，同时根据列类型来对数据进行解析。

## CSharp与Java序列化数据时的大小端差异
由于Java序列化默认大端协议，而CSharp序列化默认得到小端序列。所以我们在CSharp中序列化数据之后，需要对数据进行反转这样后端才可以正常解析。同时当我们从后端获取到序列化的结果时(如`SessionDataset`)，我们也需要对获得的数据进行反转以解析内容。这其中特例便是字符串的序列化，CSharp中对字符串的序列化结果为大端序，所以序列化字符串或者接收到字符串序列化结果时，不需要反转序列结果。

## ByteBuffer内存倍增法
拥有数万行的Tablet的序列化结果可能有上百兆，为了能够高效的实现大`Tablet`的序列化，我们对ByteBuffer使用`内存倍增法`的策略来减少序列化过程中对于内存的申请和释放。即当当前的buffer的长度不足以放下序列化结果时，我们将当前buffer的内存`至少`扩增2倍。这极大的减少了内存的申请释放次数，加速了大Tablet的序列化速度。

    private void extend_buffer(int space_need){
            if(write_pos + space_need >= total_length){
                total_length = max(space_need, total_length);
                byte[] new_buffer = new byte[total_length * 2];
                buffer.CopyTo(new_buffer, 0);
                buffer = new_buffer;
                total_length = 2 * total_length;
            }
    }
同时在序列化`Tablet`时，我们首先根据Tablet的`行数`，`列数`以及每一列的数据类型估计当前`Tablet`序列化结果所需要的内存大小，并在初始化时进行内存的申请。这进一步的减少了内存的申请释放频率。

通过上述的策略，我们在一个有`20000`行的Tablet上进行测试时，序列化速度相比Naive数组长度动态生长实现算法具有约35倍的性能加速。

