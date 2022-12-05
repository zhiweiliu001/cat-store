package com.lzw.cat.catstore;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import sun.java2d.pipe.SpanIterator;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RocketMqTestJiQun {

    public static final String nameServerAddress = "192.168.1.135:9876;192.168.1.136:9876;192.168.1.137:9876";

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
        //交付给消费逻辑的数量
        //consumer.setConsumeMessageBatchMaxSize(1);
        //从broker拉去拉取消息的数量
        consumer.setPullBatchSize(1);
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
            Select select = new Select();
            //SendResult result = producer.send(message, select, i % 3);
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

    /**
     * 广播方式
     * 1。生产者方 没有任何变化
     * 2。消费端: consumer.setMessageModel(MessageModel.BROADCASTING);
     */
    public void consumerBroadCasting() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("");
        consumer.setMessageModel(MessageModel.BROADCASTING);
    }

    /**
     * 延迟队列（超时）
     * 1 细粒度
     * 时间轮：自学一下，排序成本，分治的概念
     * <p>
     * 2 粗粒度
     * messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 18个延迟级别
     * 目标topic：oooxx_topic 消息期望 30s ，你的消息是放到 30S-topic ,由rocketmq brocker 去监听
     * 主要体现在生产方
     */
    @Test
    public void delayQueueScheduleConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delay_consumer");
        consumer.setNamesrvAddr(nameServerAddress);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_delay1", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(messageExt -> {
                    System.out.println(new String(messageExt.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        consumer.shutdown();

    }

    @Test
    public void delayQueueScheduleProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delay_producer");
        producer.setNamesrvAddr(nameServerAddress);
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("topic_delay1", "TagD", ("message" + i).getBytes());
            //延迟是消息级别的
            message.setDelayTimeLevel(1);
            SendResult result = producer.send(message);//没有直接进入到目标的topic，而是进入延迟队列
            System.out.println(result);
        }
        System.in.read();
    }

    /**
     * 事务生产
     */
    @Test
    public void tranProducer() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("tran_producer1");
        producer.setNamesrvAddr(nameServerAddress);
        //1.half半消息成功了才能执行本地事务，也需要监听
        //2.半消息的回调需被监听到
        producer.setTransactionListener(new TransactionListener() {
            //本地事务应该干啥事呢？
            //基于成本的考虑。
            //三个地方可以传导业务需求的参数
            // 1 message 的body 1.网络带宽的成本 2.编解码问题
            // 2 putUserProperty方式 1.通过网络传递给consumer
            // 3 args方式 传递
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                //send ok 半消息
                String action = message.getProperty("action");
                //String action=(String)o;
                String transactionId = message.getTransactionId();
                System.out.println("transactionID:" + transactionId);
                /*
                    状态有两个：
                        rocketmq的half消息，这个状态驱动rocketmq回查producer
                        service应该是无状态的，那么应该把transactionId随着本地事务的执行写入事件状态表中
                 */
                switch (action) {
                    case "0":
                        System.out.println(Thread.currentThread().getName() + "send half: async api call...");
                        return LocalTransactionState.UNKNOW; //rocketmq 会回调我去check
                    case "1":
                        System.out.println(Thread.currentThread().getName() + "send half: local tran fail ...");
                        /*
                        transaction.begin
                        throw...
                        transaction.rollbacl
                         */
                        //观察consumer是消费不到 rollback的message
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                    case "2":
                        System.out.println(Thread.currentThread().getName() + "send half: local tran fail ...");
                        /*
                        transaction.begin
                        transaction.commit
                         */
                        //观察consumer是肯定消费的到的，只不过还是要验证，中途会不会做check
                        try {
                            Thread.sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return LocalTransactionState.COMMIT_MESSAGE;
                }
                return null;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                //call back check
                //String transactionId = messageExt.getTransactionId();
                String action = messageExt.getProperty("action");
                int times = messageExt.getReconsumeTimes();
                switch (action) {
                    case "0":
                        System.out.println(Thread.currentThread().getName() + "check : action: 0 UNKNOW: " + times);
                        //具体业务实现。逻辑查询事件状态表，去做处理
                        return LocalTransactionState.COMMIT_MESSAGE;
                    case "1":
                        System.out.println(Thread.currentThread().getName() + "check : action: 1 ROLLBACK_MESSAGE: " + times);
                        //具体业务实现。观察事务表
                        return LocalTransactionState.UNKNOW;

                    case "2":
                        System.out.println(Thread.currentThread().getName() + "check : action: 2 COMMIT_MESSAGE: " + times);
                        //具体业务实现。观察事务表
                        return LocalTransactionState.UNKNOW;
                    //只有在未来事务状态表得到结果，做相应的commit或者rollback

                    //做了一个长时间的事务，check是要做的
                    //如果producer重启了一下，还能不能响应这个检查

                }
                return null;
            }
        });
        //需要线程，不是必须要配置
        producer.setExecutorService(new ThreadPoolExecutor(
                1,
                Runtime.getRuntime().availableProcessors(),//物理机的CPU的数量
                2000,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {//loop worker ->消费你提供的queue中的runnable
                        return new Thread(r, "tran thread ");
                    }
                }

        ));
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message("topic_tran1", "TagT", "key" + i, ("message:" + i).getBytes());
            //添加其他属性
            message.putUserProperty("action", i % 3 + "");
            TransactionSendResult result = producer.sendMessageInTransaction(message, i % 3 + "");
            System.out.println(result);
        }
        System.in.read();

    }

    /**
     * 事务消费
     */
    @Test
    public void tranConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tran_consumer");
        consumer.setNamesrvAddr(nameServerAddress);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_tran1", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(messageExt -> {
                    System.out.println(new String(messageExt.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        consumer.shutdown();

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
