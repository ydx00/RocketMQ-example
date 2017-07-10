package com.sdcuike.mq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Created by beaver on 2017/7/10.
 *
 * @see https://rocketmq.incubator.apache.org/docs/broadcast-example/
 */
public class BroadcastProducer {
    
    public static final String PRODUCER_NAME = "BroadcastProducer-demo01";
    
    public static final String NAMESRV_ADDR = "localhost:9876";
    
    public static final String TOPIC = "topic-a";
    
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_NAME);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        
        for (int i = 0; i < 10; i++) {
            Message message = new Message(TOPIC, ("hewllo" + i).getBytes(StandardCharsets.UTF_8));
            final SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
            
        }
        
        TimeUnit.MINUTES.sleep(3);
        producer.shutdown();
    }
}
