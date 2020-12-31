package org.ymx
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
object RealTimeApp1 {

	def main(args: Array[String]): Unit = {
		//idea调试使用local,集群中使用yarn
		val ss: SparkSession = SparkSession.builder.master("yarn").appName("ymx_order_app1").enableHiveSupport.getOrCreate
		val df: DataFrame = ss.sql("select * from ymx.ods_order_info")
		df.show()
		df.count
	}
}
