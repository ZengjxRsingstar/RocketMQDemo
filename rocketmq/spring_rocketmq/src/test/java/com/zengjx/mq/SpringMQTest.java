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
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  20:24
 * @Version V1.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:applicationContext-producer.xml","classpath*:applicationContext-consumer.xml"})
public class SpringMQTest {
   @Autowired
   private DefaultMQProducer  defaultMQProducer ;

   @Test
   public   void   testSendMsg()  throws Exception{
       //1.创建生产者
      // DefaultMQProducer   defaultMQProducer  =new DefaultMQProducer("productGroup");
       //2.设置NameServerd 地址
       defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
       // 3.启动生产者producer.start()
       defaultMQProducer.start();
       //4.创建消息message
       //Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK)
       Message message=new Message("topic-spring",
               "tag-spring",
               "key-spring",
               "这是我第一次发送RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
       SendResult sendResult = defaultMQProducer.send(message);
       System.out.println("sendRsult:"+sendResult);
      // defaultMQProducer.shutdown();
       //5.发送消息 接收结果sendResult
       //6.输出sendResult 查看是否成功
       //7.如果不再发送消息关闭生产者



   }


    @Test
    public void testReadMsg1(){
        try {
            //等待用户输入，阻塞结束方法
            System.out.println("等待.广播.......");
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
