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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace iotdb_client_csharp.client.utils
{
    public class ConcurentClientQueue
    {
        public ConcurrentQueue<Client> client_queue;
        public ConcurentClientQueue(List<Client> clients){
            client_queue = new ConcurrentQueue<Client>(clients);
        }
        public ConcurentClientQueue(){
            client_queue = new ConcurrentQueue<Client>();
        }
        public void Add(Client client){
            Monitor.Enter(client_queue);
            client_queue.Enqueue(client);
            Monitor.Pulse(client_queue);
            Monitor.Exit(client_queue);
        }
        public Client Take(){
            Client client;
            Monitor.Enter(client_queue);
            if(client_queue.IsEmpty){
                Monitor.Wait(client_queue);
            }
            client_queue.TryDequeue(out client);
            Monitor.Exit(client_queue);
            return client;
        }





    }
}