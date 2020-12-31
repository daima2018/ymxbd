package org.ymx.util

import java.sql.{Connection, DriverManager}
import java.util

object MySqlUtil {
	private val max = 4                         //连接池连接总数
	private val connectionNum = 5    //每次产生连接数
	private var conNum = 0                  //当前连接池已产生的连接数
	private val pool = new util.LinkedList[Connection]()    //连接池
	//获取连接
	def getJdbcConn() : Connection = {
		//同步代码块
		AnyRef.synchronized({
			if(pool.isEmpty){
				//加载驱动
				preGetConn()
				for(i <- 1 to connectionNum){
					val conn = DriverManager.getConnection("jdbc:mysql://hdp101:3306/ymx", "root", "12345678")
					pool.push(conn)
					conNum +=  1
				}
			}
			pool.poll()
		})
	}
	//释放连接
	def releaseConn(conn:Connection): Unit ={
		pool.push(conn)
	}
	//加载驱动
	private def preGetConn() : Unit = {
		//控制加载
		if(conNum > max && pool.isEmpty){
			println("Jdbc Pool has no connection now, please wait a moments!")
			Thread.sleep(2000)
			preGetConn()
		}else{
			Class.forName("com.mysql.jdbc.Driver");
		}
	}
}
