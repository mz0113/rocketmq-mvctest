package org.test.rocketmqtest.controller;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

//@Component
//@RocketMQMessageListener(topic = "test-topic",consumeMode = ConsumeMode.CONCURRENTLY, consumerGroup = "testTopigGrp1")
public class MQListener implements RocketMQListener<String> {
    {
        System.out.println("初始化MQListener");
    }
    @Override
    public void onMessage(String message) {
        System.out.printf("------- MQListener received: %s \n", message);
    }
}
