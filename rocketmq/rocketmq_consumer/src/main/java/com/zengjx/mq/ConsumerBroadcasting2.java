package com.zengjx.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  17:30
 * @Version V1.0
 */
public class ConsumerBroadcasting2 {

    /***
     * 读取同步消息
     * @param args
     */
    public static void main(String[] args) throws Exception {
           test1();
       //   testorder();
    }
   public  static    void   test1()   throws  Exception{


       //  1.创建推动式消费者-DefaultMQPushConsumer consumer = new..(consumerGroup)
       DefaultMQPushConsumer   consumer  =new DefaultMQPushConsumer("consumerGroup");
       //2. 设置NameServer 地址,如果是集群环境，则用分号隔开
       consumer.setNamesrvAddr("127.0.0.1:9876");

       //3.设置消息的主题
       consumer.subscribe("topic-1","*");
       // 设置为广播模式
       consumer.setMessageModel(MessageModel.BROADCASTING);
       //4. 创建消息监听器
       consumer.setMessageListener(new MessageListenerConcurrently() {
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
       consumer.start();//启动消费者
   }
   //读取全局消息

    public  static    void   testorder()   throws   Exception{


        //  1.创建推动式消费者-DefaultMQPushConsumer consumer = new..(consumerGroup)
        DefaultMQPushConsumer   consumer  =new DefaultMQPushConsumer("consumerGroup");
        //2. 设置NameServer 地址,如果是集群环境，则用分号隔开
        consumer.setNamesrvAddr("127.0.0.1:9876");

        //3.设置消息的主题
        consumer.subscribe("topic-order","*");
        //4. 创建消息监听器
        consumer.setMessageListener(new MessageListenerConcurrently() {
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
        try {
            consumer.start();//启动消费者
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
