package org.ymx;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class KafkaProducerApp
{
    public static DateFormat formater=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main( String[] args )
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hdp101:9092,hdp102:9092,hdp103:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        try {
            for (int i = 0; i < 10000; i++){
                Thread.sleep(500);
                JSONObject json=new JSONObject();
                json.put("orderId",Integer.toString(createOrderId()));
                json.put("time",formater.format(new Date(System.currentTimeMillis())));
                json.put("count",1);
//                String content="orderId:"+Integer.toString(createOrderId())+",time:"+
//                        formater.format(new Date(System.currentTimeMillis()))+",count:1";
                System.out.println(json.toJSONString());
                producer.send(new ProducerRecord<String,String>("ymx-order", Integer.toString(i),json.toJSONString()));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    public static int createOrderId(){
        int max=10,min=1;
        return (int) (Math.random()*(max-min)+min);
    }

}
