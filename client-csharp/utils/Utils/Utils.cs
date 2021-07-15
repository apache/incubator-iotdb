/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class Utils
    {
        public bool check_sorted(List<long> timestamp_lst){
            for(int i = 1; i < timestamp_lst.Count; i++){
                if(timestamp_lst[i] < timestamp_lst[i-1]){
                    return false;
                }
            }
            return true;
        }
        public int verify_success(TSStatus status, int SUCCESS_CODE){
            if(status.__isset.subStatus){
                foreach(var sub_status in status.SubStatus){
                    if(verify_success(sub_status, SUCCESS_CODE) != 0){
                        return -1;
                    }
                }
                return 0;
            }
            if (status.Code == SUCCESS_CODE){
                return 0;
            }
            return -1;
        }
    }
}