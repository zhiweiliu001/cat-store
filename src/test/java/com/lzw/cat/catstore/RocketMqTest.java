package com.lzw.cat.catstore;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLOutput;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@SpringBootTest
class RocketMqTest {

    @Test
    public void producer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ooxx");
        producer.setNamesrvAddr("192.168.1.135:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("wula");
            message.setTags("TagA");
            message.setBody(("ooxx" + i).getBytes());
            message.setWaitStoreMsgOK(true);
            //第一种发送方式：同步
//            SendResult result = producer.send(message);
//            System.out.println(result);

            //第二种方式方式：异步
//            producer.send(message, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    System.out.println(sendResult);
//                }
//
//                @Override
//                public void onException(Throwable throwable) {
//                }
//            });

            //第三种发送方式：
            //producer.sendOneway(message);

            //这种场景很稀缺，但是它支持，但有背分布式的特征
            MessageQueue messageQueue = new MessageQueue("wula", "node01", 0);
            SendResult result = producer.send(message, messageQueue);
            System.out.println(result);
        }
    }

    @Test
    public void consumerPull() throws Exception {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("consumer_02");
        consumer.setNamesrvAddr("192.168.1.135:9876");
        consumer.start();
        Collection<MessageQueue> mqs = consumer.fetchMessageQueues("wula");
        System.out.println("queues:");
        mqs.forEach(messageQueue -> System.out.println(messageQueue));
        System.out.println("poll...");
        consumer.assign(mqs);
        //consumer.seek(mqs,4);
        List<MessageExt> poll = consumer.poll();
        poll.forEach(messageExt -> System.out.println(messageExt));
        System.in.read();
    }

    @Test
    public void consumerPush() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_01");
        consumer.setNamesrvAddr("192.168.1.135:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("wula", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //小批量，小集合
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(messageExt -> {
                    //System.out.println(messageExt);
                    byte[] body = messageExt.getBody();
                    String msg = new String(body);
                    System.out.println(msg);
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    @Test
    public void admin() throws Exception {
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr("192.168.1.135:9876");
        admin.start();

        TopicList topicList = admin.fetchAllTopicList();
        Set<String> sets = topicList.getTopicList();
        sets.forEach(s -> System.out.println(s));

        System.out.println("-----topic route | info--------");
        TopicRouteData wula = admin.examineTopicRouteInfo("wula");
        System.out.println(wula);


    }


}
