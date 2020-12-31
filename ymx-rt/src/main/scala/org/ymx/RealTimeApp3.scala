package org.ymx

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.ymx.beans.OrderInfo
import org.ymx.data.MysqlData
import org.ymx.util.{MyKafkaUtil}

//实现累加
object RealTimeApp3 {
	def main(args: Array[String]): Unit = {
		// 定义更新状态方法，参数values为当前批次数据，state为以往批次数据
		val updateFunc = (values: Seq[Int], state: Option[Int]) => {
			val currentCount = values.foldLeft(0)(_ + _)
			val previousCount = state.getOrElse(0)
			Some(currentCount + previousCount)
		}
		System.setProperty("HADOOP_USER_NAME", "mingx") //要写在sparkContext创建之前.
		// 写在了sparkContext建立之后，此时程序已经向集群以实际的主机名注册完毕，所以修改主机名自然无效，只需要将本行代码移至开头即可
				val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app").setMaster("local[*]")
//		val sparkConf: SparkConf = new SparkConf().setAppName("ymx_order_app3").setMaster("yarn")
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc, Seconds(10)) //10秒钟统计一次   统计太快,不容易看问题
		ssc.checkpoint("hdfs://hdp101:8020/streamCheck")

		val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ymx-order", ssc)

		val orderSum: DStream[(String, Int)] = recordDstream.map(_.value()).map({ str =>
			println("-----------1--------------" + str)
			val info: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
			(info.orderId + "*" + info.time.substring(0, 16), info.count)
		}).reduceByKey(_ + _)

		val totalOrderSum: DStream[(String, Int)] = orderSum.updateStateByKey[Int](updateFunc)
		//		totalOrderSum.print(100)   //打印数据
//		val day: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

		val totalOrderSumOneDay: DStream[(String, String, Int)] = totalOrderSum.map(x => {
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

//		totalOrderSumOneDay.foreachRDD(
//			rdd =>rdd.foreachPartition(line =>{
//				val conn: Connection = MySqlUtil.getJdbcConn()
//				conn.setAutoCommit(false)
//				val sql1="delete from ymx.order where day="+day   //按天进行覆盖,如果是多个分区,当前分区的数据可能被删除
//				conn.prepareStatement(sql1).executeUpdate()
//				for(row <- line){
//					val sql2="insert into ymx.order values("+row._1+","+row._2+","+row._3+","+row._4+")"
//					conn.prepareStatement(sql2).executeUpdate()
//				}
//			})
//		)