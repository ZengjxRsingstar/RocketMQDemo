package com.zengjx.mq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  11:04
 * @Version V1.0
 */
public class ProducerOrder {
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

        for (int i = 0; i <100 ; i++) {

            String   info ="这是我"+i+"次发送全局RocketMQ";
            //4.创建消息message
            //Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK)
            Message   message=new Message("topic-order",
                    "tag-order",
                    "key-order",
                    info.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {

                    Integer   id =(Integer)o;//获取外面第三个参数的传入
                    int  index =id%list.size();//用 id 求模队列完成负载存储队列的下标
                    return    list.get(index);//存储分区对列
                }
            },18);////此参数会传入select()方法的Object arg中，一般传入唯一id
            System.out.println("sendRsult:"+sendResult);
        }

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
