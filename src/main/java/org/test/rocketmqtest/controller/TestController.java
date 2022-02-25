package org.test.rocketmqtest.controller;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/test")
public class TestController {
    //@Autowired
    RocketMQTemplate rocketMQTemplate;

    boolean flag = false;
    ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
    TransactionMQProducer  transactionMQProducer = new TransactionMQProducer();

    @RequestMapping("/sendMsg/{numOfMsg}")
    public String sendMsg(@PathVariable("numOfMsg") int numOfMsg) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        for (int i = 0; i < numOfMsg; i++) {
            defaultMQProducer.send(new Message("test-topic","testMsg".getBytes(StandardCharsets.UTF_8)));
        }
        return "ok";
    }

    @PostConstruct
    public void init() throws MQClientException {
        //defaultMQProducer.setPollNameServerInterval(1000);
        defaultMQProducer.setRetryTimesWhenSendFailed(1);
        defaultMQProducer.setProducerGroup("grp1");
        defaultMQProducer.setNamesrvAddr("172.17.39.185:9876;172.17.39.186:9876;172.17.39.187:9876");
        defaultMQProducer.start();

        transactionMQProducer.setRetryTimesWhenSendFailed(1);
        transactionMQProducer.setProducerGroup("grp2");
        transactionMQProducer.setNamesrvAddr("172.17.39.185:9876;172.17.39.186:9876;172.17.39.187:9876");
        transactionMQProducer.start();
        startConsumer();
    }

    @RequestMapping("/sendMsgInterval/{intervalms}")
    public String sendMsgInterval(@PathVariable("intervalms") int intervalms){
        transactionMQProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("executeLocalTransaction...");
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                AtomicInteger atomicInteger = new AtomicInteger(0);

                while (true) {
/*                    rocketMQTemplate.asyncSend("test-topic:*", MessageBuilder.withPayload("testMsg!!!").build(), new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            System.out.println("on success!");
                        }

                        @Override
                        public void onException(Throwable e) {
                            System.err.println("on error!");
                        }
                    });*/
                    try {

                        Message message = new Message("test-topic", "testMsg".getBytes(StandardCharsets.UTF_8));
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");
                        //SendResult sendResult = defaultMQProducer.send(new Message("test-topic", "testMsg".getBytes(StandardCharsets.UTF_8)));

/*                        SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                            @Override
                            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                                return mqs.get(atomicInteger.incrementAndGet() % mqs.size());
                            }
                        }, 2);*/


                        TransactionSendResult sendResult = transactionMQProducer.sendMessageInTransaction(message, 1);

                        if (!(sendResult.getSendStatus()== SendStatus.SEND_OK)) {
                            throw new RuntimeException("NOT OK");
                        }
                        System.out.println(simpleDateFormat.format(new Date())+" sendStatus="+sendResult.getSendStatus());
                        Thread.sleep(intervalms);
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        });

        return "ok";
    }

    @RequestMapping("/startConsumer")
    public String startConsumer(){
/*        List<String> stringList = rocketMQTemplate.receive(String.class,2000);
        for (String s : stringList) {
            System.out.println(s);
        }*/

        if (flag) {
            return "has started already!";
        }
        flag = true;
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testTopicGrp1");
        consumer.setNamesrvAddr("172.17.39.185:9876;172.17.39.186:9876;172.17.39.187:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        try {
            consumer.subscribe("test-topic","*");
        } catch (MQClientException e) {
            return e.getMessage();
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss");

                for (MessageExt msg : msgs) {
                    //如果是顺序消费的listener的话,可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                    System.out.println(simpleDateFormat.format(new Date())+" brokerName="+msg.getBrokerName() + " queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        try {
            consumer.start();
        } catch (MQClientException e) {
            return e.getMessage();
        }

        return "Consumer Started";
    }
}
