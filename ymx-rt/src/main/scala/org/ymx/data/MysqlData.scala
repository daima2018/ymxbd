package org.ymx.data

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.text.SimpleDateFormat
import java.util.Date

object MysqlData {
	def insertData(iterator: Iterator[(String, String, Int)]): Unit ={
		val conn:Connection = null
		try {
			//			val conn: Connection = MySqlUtil.getJdbcConn()  //暂时有问题
			Class.forName("com.mysql.jdbc.Driver");
			val conn = DriverManager.getConnection("jdbc:mysql://hdp101:3306/ymx", "root", "12345678")
			conn.setAutoCommit(false)
			val sql = "insert into ymx.order values(?,?,?,?,?)"
			val pstmt: PreparedStatement = conn.prepareStatement(sql)
			val today = new Date()
			val day: String = new SimpleDateFormat("yyyy-MM-dd").format(today)
			val create_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(today)
			for (row <- iterator) {
				pstmt.setString(1, row._1)
				pstmt.setString(2, row._2)
				pstmt.setInt(3, row._3)
				pstmt.setString(4, day)
				pstmt.setString(5, create_time)
				pstmt.addBatch()
			}
			pstmt.executeBatch() //执行批处理
			conn.commit()
		} catch {
			case e: Exception => e.printStackTrace()
		} finally {
			//			if (conn != null) {MySqlUtil.releaseConn(conn)}
			if (conn != null){
				try {
					conn.close()
				} catch {
					case s:SQLException => s.printStackTrace()
				}
			}
		}
	}
}
