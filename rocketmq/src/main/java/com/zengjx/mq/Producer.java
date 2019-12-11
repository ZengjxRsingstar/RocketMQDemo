package com.zengjx.mq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  11:04
 * @Version V1.0
 */
public class Producer {
    /**
     *  发送消息
     * @param args
     */

    public static void main(String[] args)   throws Exception {

       //test();
        test2();
    }
    public   static    void   test2()  throws   Exception{


        //1.创建生产者
        DefaultMQProducer   defaultMQProducer  =new DefaultMQProducer("productGroup");
        //2.设置NameServerd 地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        // 3.启动生产者producer.start()
        defaultMQProducer.start();
        //4.创建消息message
        //Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK)
        Message   message=new Message("topic-1",
                "tag-1",
                "key-1",
                "这是我第一次发送RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult sendResult = defaultMQProducer.send(message);
        System.out.println("sendRsult:"+sendResult);
        defaultMQProducer.shutdown();
        //5.发送消息 接收结果sendResult
        //6.输出sendResult 查看是否成功
        //7.如果不再发送消息关闭生产者
    }

    public static void test() throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("finance");
        // Specify name server addresses.
        //producer.setNamesrvAddr("192.168.149.212:9876;192.168.149.213:9876");
       producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setVipChannelEnabled(false);
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 100; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("1234" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes("UTF-8") /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
