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

package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.*;
import java.util.stream.Collectors;

public class AllocateMessageQueueSpecialLabel implements AllocateMessageQueueStrategy {

    private final InternalLogger log = ClientLogger.getLog();

    private double percentage = 0.2;
    private AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy;
    private String specialLabel = System.getProperty("rocketmq.consumer.label", "%GRAY%");


    public AllocateMessageQueueSpecialLabel() {
        this(new AllocateMessageQueueAveragely());
    }

    public AllocateMessageQueueSpecialLabel(double percentage) {
        this(percentage, new AllocateMessageQueueAveragely());
    }

    public AllocateMessageQueueSpecialLabel(AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy) {
        this.defaultAllocateMessageQueueStrategy = defaultAllocateMessageQueueStrategy;
    }

    public AllocateMessageQueueSpecialLabel(double percentage, AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy) {
        this.percentage = percentage;
        this.defaultAllocateMessageQueueStrategy = defaultAllocateMessageQueueStrategy;
    }

    public AllocateMessageQueueSpecialLabel(double percentage, AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy, String specialLabel) {
        this.percentage = percentage;
        this.defaultAllocateMessageQueueStrategy = defaultAllocateMessageQueueStrategy;
        this.specialLabel = specialLabel;
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }
        if (percentage < 0 || percentage > 1) {
            throw new IllegalArgumentException("percentage must be between 0 and 1");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);
            return result;
        }

        Map<String, List<MessageQueue>> brokerQueuesTables = mqAll.stream().collect(Collectors.toMap(mq -> mq.getBrokerName(), mq -> {
            List<MessageQueue> brokerQueues = new ArrayList<>();
            brokerQueues.add(mq);
            return brokerQueues;
        }, (brokerQueuesFirst, brokerQueuesSecond) -> {
            brokerQueuesFirst.addAll(brokerQueuesSecond);
            return brokerQueuesFirst;
        }));

        List<MessageQueue> specialLabelMessageQueueList = new ArrayList<>();

        brokerQueuesTables.forEach((brokerName, brokerQueueList) -> {
            int size = brokerQueueList.size();
            int queueSizePerBroker = (int) Math.ceil(size * percentage);
            Collections.sort(brokerQueueList);
            for (int i = 0; i < queueSizePerBroker; i++) {
                specialLabelMessageQueueList.add(brokerQueueList.get(i));
            }
        });

        specialLabelMessageQueueList.forEach(specialLabelMq -> mqAll.remove(specialLabelMq));
        List<String> specialLabelCidAll = cidAll.stream().filter(cid -> cid.contains(specialLabel)).filter(Objects::nonNull).collect(Collectors.toList());
        specialLabelCidAll.forEach(specialLabelCid -> cidAll.remove(specialLabelCid));


        Collections.sort(specialLabelCidAll);
        Collections.sort(specialLabelMessageQueueList);

        if (currentCID.contains(specialLabel)) {
            return defaultAllocateMessageQueueStrategy.allocate(consumerGroup, currentCID, specialLabelMessageQueueList, specialLabelCidAll);
        } else {
            return defaultAllocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }
    }

    @Override
    public String getName() {
        return "Special Label";
    }


}
