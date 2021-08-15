/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.ha;


import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class HAPutCommitLogService {


    private Set<HAConnection> connections = new ConcurrentSkipListSet<>();
    private ExecutorService executorService;
    private MessageStoreConfig messageStoreConfig;
    private AtomicLong nextTransferFromWhere;

    public HAPutCommitLogService(MessageStoreConfig messageStoreConfig) {
        nextTransferFromWhere = new AtomicLong(-1);
        this.messageStoreConfig = messageStoreConfig;
        executorService = Executors.newFixedThreadPool(this.messageStoreConfig.getHaClientCount());
    }

    public void addConnection(HAConnection connection) {
        connections.add(connection);
        executorService.submit(new PutCommitLogTask(connection));
    }

    public void removeConnection(HAConnection connection) {
        connections.remove(connection);
        connection.setStopFlag(true);
    }

    public Set<HAConnection> getConnections() {
        return connections;
    }

    class PutCommitLogTask implements Runnable {

        private HAConnection connection;

        public PutCommitLogTask(HAConnection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            long nextFromWhere = connection.getWriteSocketService().doWriteMethod(nextTransferFromWhere.longValue());
            nextTransferFromWhere.updateAndGet(origin -> Math.max(nextFromWhere, origin));
            if (!connection.isStopFlag()) {
                executorService.submit(new PutCommitLogTask(connection));
            }
        }
    }

}
