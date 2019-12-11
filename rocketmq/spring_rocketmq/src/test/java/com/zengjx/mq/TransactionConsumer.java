package com.zengjx.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/10  13:16
 * @Version V1.0
 */

public class TransactionConsumer {


    public static void main(String[] args) throws Exception {
         test();
    }
    public    static void   test()  throws  Exception{
     //1.创建DefaultMQPushConsumer
     //2. 设置nameServer地址
     //3.设置消费者顺序
        //设置每次拉取的消息个数
     //4.设置监听的消息
     //5.消息监听
        DefaultMQPushConsumer    defaultMQPushConsumer  =new DefaultMQPushConsumer("transation_consumerGroup") ;
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(5);
        defaultMQPushConsumer.subscribe("topic-trans","*");
        //4. 创建消息监听器
        defaultMQPushConsumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                Iterator<MessageExt> iterator = list.iterator();
                while (iterator.hasNext())
                {
                    MessageExt messageExt = iterator.next();
                    String topic = messageExt.getTopic();//主题
                    String tags = messageExt.getTags();//标签
                    String keys = messageExt.getKeys();//消息keys

                    //消息内容
                    String   body    = null;
                    try {
                        body = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    System.out.println("topic "+topic+ " tags"+tags+ " keys "+keys+" body"+body);
                }
                //返回消息读取结果
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        defaultMQPushConsumer.start();//启动消费者



    }




}
