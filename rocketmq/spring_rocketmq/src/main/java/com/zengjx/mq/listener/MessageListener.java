package com.zengjx.mq.listener;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/9  21:20
 * @Version V1.0
 */
public class MessageListener    implements MessageListenerConcurrently {
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        try {
            //4.1.循环读取消息-msgs.for
            for (MessageExt msg : list) {
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
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }



}
