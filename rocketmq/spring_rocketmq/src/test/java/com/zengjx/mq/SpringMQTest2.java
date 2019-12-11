package com.zengjx.mq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  21:49
 * @Version V1.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:applicationContext-producer.xml","classpath*:applicationContext-consumer.xml"})
public class SpringMQTest2 {
    @Autowired
    private DefaultMQProducer producer;

    @Test
    public void testSendMsg() throws Exception{
        //4.创建消息-message = new Message(主题名,标签名,消息key名,消息内容.getBytes(RemotingHelper.DEFAULT_CHARSET));
        Message message = new Message(
                "topic-spring",
                "tag-1",
                "key-1",
                "这是我第1次发送Spring-MQ消息".getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        //5.发送消息，接收结果-sendResult = producer.send(message)
        SendResult sendResult = producer.send(message);
        //6.输出sendResult查看消息是否成功送达
        System.out.println(sendResult);
    }
}
