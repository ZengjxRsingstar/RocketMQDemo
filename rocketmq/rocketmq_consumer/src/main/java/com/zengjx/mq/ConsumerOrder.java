import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * rocketmq入门-消费者-全局有序
 * @author Steven
 * @description com.itheima.mq
 */
public class ConsumerOrder {
    /**
     * 读取全局有序消息
     * @param args
     */
    public static void main(String[] args) throws Exception{
        //1.创建推动式消费者-DefaultMQPushConsumer consumer = new..(consumerGroup)
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroup");
        //2.设置NameServer地址，如果是集群环境，则用分号隔开-setNameservAddr
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //3.设置消息的主题(与生产者要一至)与标签(*号代表所有)-consumer.subscribe("topic-1","*")
        consumer.subscribe("topic-order","*");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //4.创建消息监听器-consumer.setMessageListener(new MessageListenerConcurrently(){})
        consumer.setMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                try {
                    //4.1.循环读取消息-msgs.for
                    for (MessageExt msg : msgs) {
                        //4.1.1输出消息-主题、标签、消息key、内容(body)
                        String topic = msg.getTopic();  //主题
                        String tags = msg.getTags();  //标签
                        String keys = msg.getKeys();  //消息key
                        //内容
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("topic:" + topic + ",tag:" + tags + ",key:" + keys + ",body:" + body);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                //4.2.返回消息读取状态-CONSUME_SUCCESS
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //5.启动消费者-consumer.start()
        consumer.start();
    }
}
