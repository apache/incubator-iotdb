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
package org.apache.iotdb.cluster.server.handlers.caller;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AppendNodeEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest.
 * The target of the log is the single nodes, it requires the agreement from the quorum of the nodes
 * to reach consistency.
 */
public class AppendNodeEntryHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(AppendNodeEntryHandler.class);

  private AtomicLong receiverTerm;
  private Log log;
  private AtomicInteger voteCounter;
  private AtomicBoolean leaderShipStale;
  private Node receiver;

  @Override
  public void onComplete(Long response) {
    logger.debug("Append response {} from {}", response, receiver);
    if (leaderShipStale.get()) {
      // someone has rejected this log because the leadership is stale
      return;
    }
    long resp = response;
    synchronized (voteCounter) {
      if (resp == RESPONSE_AGREE) {
        int remaining = voteCounter.decrementAndGet();
        logger.debug("Received an agreement from {} for {}, remaining votes to succeed: {}",
            receiver, log, remaining);
        if (remaining == 0) {
          logger.debug("Log {} is accepted by the quorum", log);
          voteCounter.notifyAll();
        }
      } else if (resp > 0) {
        // a response > 0 is the follower's term
        // the leader ship is stale, wait for the new leader's heartbeat
        long prevReceiverTerm = receiverTerm.get();
        logger.debug("Received a rejection from {} because term is stale: {}/{}", receiver,
            prevReceiverTerm, resp);
        if (resp > prevReceiverTerm) {
          receiverTerm.set(resp);
        }
        leaderShipStale.set(true);
        voteCounter.notifyAll();
      } else {
        //e.g., Response.RESPONSE_LOG_MISMATCH
        //But it is impossible that more than quorum nodes return RESPONSE_LOG_MISMATCH.
        logger.debug("The log {} is rejected because: {}", log, resp);
      }
      // rejected because the receiver's logs are stale or the receiver has no cluster info, just
      // wait for the heartbeat to handle
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.debug("Cannot connect to {}: {}", receiver, exception.getMessage());
    } else {
      logger.warn("Cannot append log {} to {}", log, receiver, exception);
    }
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public void setVoteCounter(AtomicInteger voteCounter) {
    this.voteCounter = voteCounter;
  }

  public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
    this.leaderShipStale = leaderShipStale;
  }

  public void setReceiver(Node follower) {
    this.receiver = follower;
  }

  public void setReceiverTerm(AtomicLong receiverTerm) {
    this.receiverTerm = receiverTerm;
  }
}
