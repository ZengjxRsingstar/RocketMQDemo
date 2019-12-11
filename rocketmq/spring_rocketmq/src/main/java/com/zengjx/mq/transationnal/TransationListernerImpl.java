package com.zengjx.mq.transationnal;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/10  11:12
 * @Version V1.0
 */
public class TransationListernerImpl  implements TransactionListener {
   //  //存储当前线程对应的事务状态-线程安全的Map
    private ConcurrentHashMap<String,Integer>  localTrans  =new ConcurrentHashMap<>();

    /***
     * 发送prepare消息成功后回调该方法用于执行本地事务
     * @param msg:回传的消息，利用transactionId即可获取到该消息的唯一Id
     * @param arg:调用send方法时传递的参数，当send时候若有额外的参数可以传递到send方法中，这里能获取到
     * @return
     */


    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
       // 1.获取线程 id
        //2/ 初始状态 0
       //3.此处执行本地事务
       //4.发生异常回滚消息
       //5.修改状态
      //6.本地事务如果提交成功则提交消息让该消息可见
        String transactionId = message.getTransactionId();
        localTrans.put(transactionId,0);
        try {
            System.out.println("执行本地事务.....1");
            Thread.sleep(1000);
            System.out.println("执行本地事务.....2");
        } catch (InterruptedException e) {
            e.printStackTrace();
            //发生异常回滚
            localTrans.put(transactionId,2);
            //返回中间状态代表需要检查消息来确认。

            return   LocalTransactionState.UNKNOW;
        }
        //修改状态
        localTrans.put(transactionId,1);
        System.out.println("executeLocalTransaction------状态为1");
//   本地事务如果操作成功则提交消息
        return LocalTransactionState.UNKNOW;
    }

    /***
     * 消息回查-用于检查本地事务状态，并回应消息队列的检查请求
     * @param msg
     * @return
     */

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        //1.获取事务id
        //2.通过事务id 获取对应本地事务执行状态

        //获取事务id
        String transactionId = messageExt.getTransactionId();
        //通过事务id获取对应的本地事务执行状态
        Integer status = localTrans.get(transactionId);
        System.out.println("消息回查-----"+status);
        switch (status){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }


        return LocalTransactionState.UNKNOW;
    }
}
