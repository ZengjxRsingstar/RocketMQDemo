package com.zengjx.mq;

import com.sun.corba.se.impl.orbutil.threadpool.WorkQueueImpl;
import com.sun.corba.se.spi.orbutil.threadpool.WorkQueue;
import com.zengjx.mq.transationnal.TransationListernerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.*;

/**
 * @ClassName HelloController
 * @Description TODO
 * @Author zengjx
 * @Company zengjx
 * @Date 2019/12/10  11:05
 * @Version V1.0
 */


public class TransztionProducer {


    public static void main(String[] args) throws Exception {
        test1();
    }
   public    static void   test1()  throws  Exception{
    //1.创建事务发送消息
    //2.设置nameAdrr
    //3. 创建监听器
    //4. 创建线程池
    //5.设置线程池
    //6.设置监听器
    //7. 启动producer
    //8.创建消息
    //9.休眠
    //10.关闭

        TransactionMQProducer  producer  =new TransactionMQProducer("tansacntion_producerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        TransactionListener  transactionListener =new TransationListernerImpl();
        //创建线程池
        ExecutorService    executorService  =new ThreadPoolExecutor(2, 5, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {

                Thread  thread  =new Thread(r);
                thread.setName("TransactionMQProducer ----threadPoolExecutor ");
                return thread;
            }
        });
        //设置线程池
        producer.setExecutorService(executorService);
        //设置监听器
        producer.setTransactionListener(transactionListener);
        //3设置启动
        producer.start();
        //创建消息
        Message   message  =new
        Message(
                "topic-trans",
                "tags-trans-a",
                "keys-trans-a",
                "hello".getBytes(RemotingHelper.DEFAULT_CHARSET));
     //发送事务消息此时消息不可见
        TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, "发送消息回传数据");
        System.out.println("sendResult:"+transactionSendResult);
        Thread.sleep(120000);
         producer.shutdown();
    }
}
