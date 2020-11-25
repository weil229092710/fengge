import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xuehai.utils.{Constants, MysqlUtils, Utils}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import java.util.concurrent.TimeUnit

import scala.collection.mutable

//import AppMain.{jobName, log}
import org.apache.flink.api.scala._
//import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.client._
//import org.apache.solr.client.solrj.impl.CloudSolrServer
//import org.apache.solr.common.SolrInputDocument

/**
  * Created by root on 2020/4/30.
  */
object OnlineMain extends Constants {
	def main(args: Array[String]) {
		onlineData()
	}

	def onlineData(): Unit ={
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.enableCheckpointing(10 * 1000)//开启checkPoint，并且每分钟做一次checkPoint保存
//		env.setStateBackend(new FsStateBackend(checkPointPath))
//		env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//		env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//		env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔1分钟重启一次，尝试重启100次
		val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](onlinetopic, new SimpleStringSchema(), props)
		env.addSource(kafkaConsumer)
	// 从文件中读取数据
	//val inputPath = "D:\\xh\\qingzhou-task\\qingzhouStream\\src\\main\\resources\\online.txt"
	//	val inputDataSet = env.readTextFile(inputPath)

			.map(x => {
					try{
						println(x)
						val bb=x.split("\\s+")
						val time=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
						val clientid=bb(9)
						val online=bb(11).split(",")(0).replace("\"","")
						//val sql="select sSchoolName from XHSchool_Info where scountyname='青州市' and bdelete=0 and istatus in (1,2) and ischoolid="+school_id
            val quUseridAndDeviceSql="select userId,deviceId from xh_user_service.XHUsers_Clients WHERE clientId='"+clientid+"' limit 1"
						val results: ResultSet = MysqlUtils.select(quUseridAndDeviceSql)
						var school_name=""
						var school_id=""
						var user_id=""
						var device_id=""
						var city=""
						var userType=""
						var num=0
						//var user_id=emptyMap.get(clientid).getOrElse("default").toString
						val	hour= time.split(" ")(1).split(":")(0).toInt
						while(results.next()){
							user_id= results.getString(1)
							device_id = results.getString(2)
						}
						//if(user_id==""){
						//	println(clientid)
						//	println("到底哪个clientid的userid为空")
						//}
						val quSchIdAndSchNameSql="select iSchoolId,school_name ,iUserType from xh_user_service.XHSys_User where iUserId='"+user_id +"'"
						val userResults: ResultSet = MysqlUtils.select(quSchIdAndSchNameSql)
						while(userResults.next()){
							school_id = userResults.getString(1)
							school_name = userResults.getString(2)
							userType=userResults.getString(3)
						}
						val quNaemeAndCitySql="select distinct scountyname from xh_user_service.XHSchool_Info where  bdelete=0 and istatus in (1,2)and ischoolid='"+school_id +"'"
						val cityResults: ResultSet = MysqlUtils.select(quNaemeAndCitySql)
						while(cityResults.next()){
							city = cityResults.getString(1)
						}
						if(online=="online"){
							num=1
						}
						else if(online=="offline"){
							num=0-1
						}

						OnlineCount(user_id,school_id,school_name,userType,city,hour,time,num)
					}
					catch {
						case e: Exception => {
							//Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
							log.error("在线人数据异常：%s, \\r\\n %s".format(e, x))
							OnlineCount("","","","","",0,"",0)
						}}
				})


  		//.filter(_.city=="青州市")
  		.filter(_.school_id!="")
  		//.filter(_.userType=="1")


			.addSink(new  MyJdbcSink())
  		//	.print()
		  env.execute(jobName)

	}
}





class MyJdbcSink() extends RichSinkFunction[OnlineCount] with Constants{
	// 定义sql连接、预编译器
	var conn: Connection = _
	var insertStmt: PreparedStatement = _
	//var updateStmt: PreparedStatement = _

	// 初始化，创建连接和预编译语句
	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		try {
			Class.forName("com.mysql.jdbc.Driver")
			conn = DriverManager.getConnection(Url, User, Password)
			insertStmt = conn.prepareStatement("INSERT INTO online_num (school_id, school_name,time,num,user_type) VALUES (?,?,?,?,?) on duplicate key update num=case when num+? <0 then 0 else num+? end,time=?")

		}
		catch {
			case e: Exception => {
				println("云mysql连接失败")
			}
		}
	}
	// 调用连接，执行sql
	override def invoke(value: OnlineCount, context: SinkFunction.Context[_]): Unit = {
		var num1=value.num
		if(num1<0){
			num1=0
		}

		try{
			// 如果update没有查到数据，那么执行插入语句
			insertStmt.setString(1, value.school_id)
			insertStmt.setString(2, value.school_name)
			insertStmt.setString(3, value.time)
			insertStmt.setInt(4, num1)
			insertStmt.setInt(5, value.userType.toInt)
			insertStmt.setInt(6,value.num)
			insertStmt.setInt(7,value.num)
			insertStmt.setString(8, value.time)
			insertStmt.execute()

		}
		catch {
			case e: Exception => {
				println("在线人数插入失败"+e+value)
			}
		}

	}


	// 关闭时做清理工作
	override def close(): Unit = {
		try{
			insertStmt.close()

			conn.close()
			println("云mysql关闭成功")
		}catch {
			case e: Exception => {
				println("云mysql关闭失败")
			}
		}
	}
}


case class OnlineCount(userid: String,
										   school_id:String,
										   school_name: String,
											 userType:String,
											 city:String,
										   hour: Int,
											 time: String,
											 num:Int
											)