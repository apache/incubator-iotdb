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

## ����
* GoLang 1.13+

## ����IoTDB

�μ�https://iotdb.apache.org/zh/UserGuide/Master/Get%20Started/QuickStart.html

## Join Prometheus

����֧��session,jdbc,cliд��������, IoTDB��֧��ͨ��API�������������Prometheus������. ������Prometheus����������.
���ɽ�����ֱ��д��IoTDB, ���������Զ�����ʱ��, �����κδ��룬�Լ���ǰ��IoTDB���κ�����.

## ʹ��prometheus_connector (��Դ�����)

```
git clone https://github.com/apache/iotdb.git
cd iotdb/prometheus_connector
go mod init iotdb_prometheus
go build
run write_adaptor.go
```

## Prometheus

Prometheus��ΪCloud Native Computing Fundation��ҵ����Ŀ�������ܼ���Լ�K8S���ܼ���������ŷǳ��㷺��Ӧ�á�IoTDB����ͨ��prometheus_connector�������ʵ���޴���Ŀ��ٽ��룬��Ч�Ľ�����д��IoTDB�С���ͨ��Grafana����ѯIoTDB�е�����

### ��װ prometheus

ͨ��Prometheus�Ĺ������ذ�װ https://prometheus.io/download/

### ���� prometheus

�ο�prometheus[�����ļ�](https://prometheus.io/docs/prometheus/latest/configuration/configuration/), add the following configuration in prometheus.yml
(ip,�˿ںźʹ洢������������conf.yaml����)

```
remote_write: 
      - url: "http://localhost:12345/receive"
```

### ���� prometheus

## �鿴����

```
select * from root.system_p_sg1
```




