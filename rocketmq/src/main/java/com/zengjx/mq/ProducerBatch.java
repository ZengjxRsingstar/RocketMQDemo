package com.zengjx.mq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;
//广播模式消费
/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  19:54
 * @Version V1.0
 */
public class ProducerBatch {
    //批量发送
    public static void main(String[] args)  throws Exception  {
       test1();
    }
    public static void test1() throws Exception {
        //1.创生产者 producer  =new
        //  2.设置NameServer
        //3. 启动生产者
        //4.创建消息  message
        //5.发送消息  接收  sendResult
        //6. 输出sendResult
        //7. 如果不发送消息关闭生产者
        DefaultMQProducer producer = new DefaultMQProducer("producerGroup");//组名
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        List<Message> messageList = new ArrayList<Message>();
        //4.创建消息-message = new Message(主题名,标签名,消息key名,消息内容.getBytes(RemotingHelper.DEFAULT_CHARSET));
        for (int i = 0; i < 10; i++) {
            Message message = new Message(
                    "topic-1",
                    "tag-" + i,
                    "key-" + i,
                    "这是我第2次发送MQ消息".getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            messageList.add(message);
        }
        SendResult sendResult = producer.send(messageList);
        System.out.println(sendResult);

        producer.shutdown();
    }



}