package org.ymx

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.ymx.beans.OrderInfo
import org.ymx.util.MyKafkaUtil

import java.text.SimpleDateFormat
//没有实现累加
object RealTimeApp2 {

	def main(args: Array[String]): Unit = {
		val dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm")
//		val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app").setMaster("local[*]")   //在idea使用local,在集群中使用yarn模式
		val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app2").setMaster("yarn")
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc,Seconds(5))

		val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ymx-order",ssc)

		val orderInfoDstream: DStream[OrderInfo] = recordDstream.map(_.value()).map { str =>
//			val arr = str.split(",")
//			//将时间变成分钟或者小时
//			OrderInfo(arr(0),arr(1).substring(0,16),arr(2).toInt)
			println("-----------1--------------"+str)
			val info:OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
			OrderInfo(info.orderId,info.time.substring(0,16),info.count)
		}

		orderInfoDstream.foreachRDD(
			rdd => {
				val orderTimeOne = rdd.map(x => (x.orderId +"*"+ x.time, x.count))
				val orderTimeCount = orderTimeOne.reduceByKey(_ + _)
				orderTimeCount.collect().foreach(println)
			}
		)

		ssc.start()
		ssc.awaitTermination()
	}
}

//val str = "{\"orderId\":27,"+"\"time\":\"2020-12-29 11:02:14\","+"\"count\":1}"
//println(str)

//		System.setProperty("HADOOP_USER_NAME", "mingx")
//		System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-3.1.3")
//val ss=SparkSession.builder().master("local").appName("ymx_order").enableHiveSupport().getOrCreate()
//val df = ss.sql("select * from ymx.ods_order_info")
//df.show()
//df.count()


