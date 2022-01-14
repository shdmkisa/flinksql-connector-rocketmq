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

package org.apache.rocketmq.flink.legacy.example;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    // Consumer config
    private static final String NAME_SERVER_ADDR =
            "http://risen-cdh01:9876";
    private static final String GROUP = "test";
    private static final String TOPIC = "TopicTest";
    private static final String TAGS = "*";

    private static RPCHook getAclRPCHook() {
        final String accessKey = "${AccessKey}";
        final String secretKey = "${SecretKey}";
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer(
                        GROUP, null, new AllocateMessageQueueAveragely());
        consumer.setNamesrvAddr(NAME_SERVER_ADDR);

        // When using aliyun products, you need to set up channels
        //consumer.setAccessChannel(AccessChannel.CLOUD);

        try {
            consumer.subscribe(TOPIC, TAGS);
            consumer.setPullInterval(1000*3);
            consumer.setPullBatchSize(300);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(
                (MessageListenerConcurrently)
                        (msgs, context) -> {
                            for (MessageExt msg : msgs) {
                                System.out.println(new String(msg.getBody()));
                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        });

        try {
            consumer.start();
        } catch (MQClientException e) {
            LOGGER.info("send message failed. {}", e.toString());
        }
    }
}
