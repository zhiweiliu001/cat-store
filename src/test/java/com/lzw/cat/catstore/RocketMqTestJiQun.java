package com.lzw.cat.catstore;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import sun.java2d.pipe.SpanIterator;

import java.util.*;

@SpringBootTest
class RocketMqTestJiQun {

    public static final String nameServerAddress = "192.168.1.135:9876;192.168.1.136:9876;192.168.1.137:9876";

    /**
     * 批量发送
     *
     * @throws Exception
     */
    @Test
    public void producer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer_111");
        producer.setNamesrvAddr(nameServerAddress);
        producer.start();
        List<Message> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Message("bala", "TagA", "key" + i,//全局唯一
                    ("message" + i).getBytes()));
        }
        SendResult result = producer.send(list);
        System.out.println(result);
    }


    /**
     * messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 18个延迟级别
     * 场景一：
     * ConsumeConcurrentlyStatus.RECONSUME_LATER
     * consumer.setMaxReconsumeTimes(2);
     * %RETRY%consumer_xxxx1 重试队列
     * %DLQ%consumer_xxxx1 死信队列
     * 总结：rocketmq ：以topic 形式，且，以consumerGroup
     * Kafka：客户端自己维护重试/死信队列
     * 场景二：
     * 延迟队列场景
     *
     * @throws Exception
     */
    @Test
    public void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_xxxx1");
        consumer.setNamesrvAddr(nameServerAddress);
        consumer.subscribe("bala", "*");
        //每次都从第一个偏移量消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //消费失败， 重试次数
        consumer.setMaxReconsumeTimes(2);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt messageExt = list.get(0);
                try {
                    System.out.println(new String(messageExt.getBody()));
                    if (messageExt.getKeys().equals("key1")) {
                        int a = 1 / 0;
                    }
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //new MessageListenerOrderly 有序消息处理
        //new MessageListenerConcurrently 无序消息处理
        consumer.start();
        System.in.read();
        consumer.shutdown();

    }


    /**
     * 有序产生
     * 保证有序性：
     * 1。producer，其实是自定义分区器
     * 2。consumer，MessageListenerOrderly
     * <p>
     * 感知：生产的时候，有顺序依赖的message进入一个队列，并且。消费者也会在多线程的情况下保证单线程对应的queue能按顺序消费
     * 如果你只创建一个queue，就是全局有序
     */
    @Test
    public void orderProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_producer");
        producer.setNamesrvAddr(nameServerAddress);
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("order_topic", "TagA", ("message body :" + i + "type: " + i % 3).getBytes());
            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer index = (Integer) o;
                    Integer target = index % list.size();
                    return list.get(target);
                    //MessageQueue target = list.get(index);
                    //return target;

                    /*
                     *  不稳定因素：分区器、选择器、路由器 代码逻辑要简单、快速
                     */
                }
            }, i % 3);
            System.out.println(result);
        }

    }

    /**
     * 有序消费
     */
    @Test
    public void orderConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_0003");
        consumer.setNamesrvAddr(nameServerAddress);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("order_topic", "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                list.forEach(s -> {
                    System.out.println(Thread.currentThread().getName() + ": " + new String(s.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        consumer.shutdown();
    }


    @Test
    public void admin() throws Exception {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(nameServerAddress);
        admin.start();

        ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
        Set<Map.Entry<String, BrokerData>> entries = brokerAddrTable.entrySet();
        Iterator<Map.Entry<String, BrokerData>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, BrokerData> next = iterator.next();
            System.out.println(next.getKey() + " @@@@ " + next.getValue());
        }

        TopicList topicList = admin.fetchAllTopicList();
        topicList.getTopicList().forEach(s -> {
            System.out.println(s);
        });


    }

    class Select implements MessageQueueSelector {

        @Override
        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
            Integer index = (Integer) o;
            Integer target = index % list.size();
            return list.get(target);
            //MessageQueue target = list.get(index);
            //return target;
        }
    }


}
