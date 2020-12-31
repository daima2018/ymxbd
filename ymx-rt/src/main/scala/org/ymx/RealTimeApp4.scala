package org.ymx

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.ymx.beans.OrderInfo
import org.ymx.data.MysqlData
import org.ymx.util.MyKafkaUtil
//不累加
object RealTimeApp4 {
	def main(args: Array[String]): Unit = {
		System.setProperty("HADOOP_USER_NAME", "mingx") //要写在sparkContext创建之前.
//		val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app4").setMaster("local[*]")
		val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app4").setMaster("yarn")
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc, Seconds(10)) //10秒钟统计一次   统计太快,不容易看问题

		val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ymx-order", ssc)

		val orderSum: DStream[(String, Int)] = recordDstream.map(_.value()).map({ str =>
			println("-----------1--------------" + str)
			val info: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
			(info.orderId + "*" + info.time.substring(0, 16), info.count)
		}).reduceByKey(_ + _)

		val totalOrderSumOneDay: DStream[(String, String, Int)] = orderSum.map(x => {
			val strings: Array[String] = x._1.split("\\*")
			(strings(0), strings(1), x._2)
		})

		totalOrderSumOneDay.foreachRDD(
			rdd => rdd.foreachPartition(line => {
				if(line.nonEmpty){
					MysqlData.insertData(line)
				}
			})
		)

		ssc.start()
		ssc.awaitTermination()
	}
}
