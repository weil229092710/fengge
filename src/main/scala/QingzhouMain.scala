//package com.xuehai.utils
//
//import java.sql.{Connection, PreparedStatement, ResultSet}
//import java.text.SimpleDateFormat
//import java.util.Locale
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.AllWindowFunction
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import java.util.concurrent.TimeUnit
//
//import org.apache.flink.api.common.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//
//import scala.collection.mutable
//
//
//
//object QingzhouMain extends Constants{
//
//  def main(args: Array[String]) {
//    taskmain()
//  }
//  def taskmain(): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //env.enableCheckpointing(20 * 1000)//开启checkPoint，并且每分钟做一次checkPoint保存
//    //env.setStateBackend(new FsStateBackend(checkPointPath))
//    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    //env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    //env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
//    val quUserInfoSql="select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname  from \nxh_user_service.XHSys_User a\nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"
//    var emptyMap = new mutable.HashMap[Int,JSON]()
//    val results: ResultSet = MysqlUtils.select(quUserInfoSql)
//
//
//    while(results.next()){
//      val  json = JSON.parseObject("{}")
//      val user_id=results.getInt(1)
//      json.put("user_id",results.getInt(1))
//      json.put("school_id",results.getString(2))
//      json.put("user_name",results.getString(3))
//      json.put("user_type",results.getInt(4))
//      json.put("school_name",results.getString(5))
//      json.put("city",results.getString(6))
//      emptyMap+=(user_id -> json)
//    }
//    // 用相对路径定义数据源
//    //val resource = getClass.getResource("/hello.txt")
//    // val dataStream = env.readTextFile(resource.getPath)
//    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
//    env.addSource(kafkaConsumer)
//      .filter(x=>{
//        try {
//          val json = JSON.parseObject(x)
//          val userid = json.getString("UserId")
//          userid != "-"
//        }catch {
//          case e: Exception => {
//            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
//            log.error("数据异常：%s, \\r\\n %s".format(e, x))
//            false
//          }
//        }
//      })
//      .map(x => {
//        try{
//          //println(x)
//          val json=	JSON.parseObject(x)
//          val	userid=json.getString("UserId")
//          val	request=json.getString("request")
//          val	appName=json.getString("appName")
//          val method=json.getString("method")
//
//          val	school_id=JSON.parseObject(emptyMap(userid.toInt).toString ).get("school_id").toString
//          val school_name=JSON.parseObject(emptyMap(userid.toInt).toString ).get("school_name").toString
//          val scountyName=JSON.parseObject(emptyMap(userid.toInt).toString ).get("city").toString
//          val userType=JSON.parseObject(emptyMap(userid.toInt).toString ).get("user_type").toString
//          val userName=JSON.parseObject(emptyMap(userid.toInt).toString ).get("user_name").toString
//          val time_local=json.getString("time_local")
//          //val time=Utils.toLoclTime(time_local)
//          val sdf1 = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
//          val date = sdf1.parse(time_local)
//          val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//          val time=	sdf2.format(date)
//          val	hour= time.split(" ")(1).split(":")(0).toInt
//
//
//          val timeStamp = System.currentTimeMillis();
//
//          task1(userid,userName,school_id,school_name,scountyName,hour.toString,time,userType,request,appName,method,timeStamp)
//        }
//        catch {
//          case e: Exception => {
//            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
//            log.error("数据异常：%s, \\r\\n %s".format(e, x))
//            task1("","","","","","","","","","","",0)
//          }
//        }
//      })
//      .filter(_.userType!="")
//      .filter(_.city=="青州市")   //只保存青州市的数据
//      .filter(_.school_name!="")
//
//      .assignAscendingTimestamps(_.timestap*1000)
//
//      .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(20))
//
//      .apply(new ByWindow())
//      .addSink(new  MySqlSink2())
//
//    // .print()
//
//    env.execute("qingzhou job")
//  }
//}
//
//class ByWindow() extends AllWindowFunction[task1, Iterable[task1], TimeWindow]{
//  override def apply(window: TimeWindow, input: Iterable[task1], out: Collector[Iterable[task1]]): Unit = {
//
//
//    if(input.nonEmpty) {
//      // System.out.println("1 秒内收集到 接口的条数是：" + input.size)
//      out.collect(input)
//    }
//
//  }
//}
//
//
//case class task1(userid: String,
//                 user_name:String,
//                 school_id:String,
//                 school_name: String,
//                 city: String,
//                 hour: String,
//                 time: String,
//                 userType:String,
//                 request:String,
//                 appName:String,
//                 method:String,
//                 timestap:Long
//                )
//
//case class userInfo(userid: Int,
//                    user_name:String,
//                    school_id:String,
//                    school_name: String,
//                    city: String,
//                    userType:Int
//                   )
//
//
//
//case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
//
//
//class MySqlSink2() extends RichSinkFunction[Iterable[task1]] with Constants {
//  // 定义sql连接、预编译器
//  var conn: Connection = _
//  var insertStmt: PreparedStatement = _
//  var insertStmtStu: PreparedStatement = _
//  var result: ResultSet = null
//  var updateStmt: PreparedStatement = _
//  var status = ""
//  import org.apache.commons.dbcp2.BasicDataSource
//
//  var dataSource: BasicDataSource = null
//
//  // 初始化，创建连接和预编译语句
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    try {
//      //Class.forName("com.mysql.jdbc.Driver")
//      //conn = DriverManager.getConnection(Url, User, Password)
//      import org.apache.commons.dbcp2.BasicDataSource
//
//      dataSource = new BasicDataSource
//      conn = getConnection(dataSource)
//      conn.setAutoCommit(false) // 开始事务
//
//      insertStmt = conn.prepareStatement("INSERT INTO real_class_copy (user_id, user_name,status,time,school_name,school_id,user_type) VALUES (?,?,?,?,?,?,?)")
//      // insertStmtStu=conn.prepareStatement("INSERT INTO active_student_num_copy (school_id, school_name,hour,time,count,user_type) VALUES (?,?,?,?,?,?) on duplicate key update count=count+1,time=?")
//      insertStmtStu=conn.prepareStatement("INSERT INTO active_student_num (school_id, school_name,hour,time,count,user_type,cur_date) VALUES (?,?,?,?,?,?,?) on duplicate key update count=count+1,time=?")
//
//    }
//    catch {
//      case e: Exception => {
//        println("云mysql连接失败")
//      }
//    }
//  }
//
//  // 调用连接，执行sql
//  override def invoke(values: Iterable[task1], context: SinkFunction.Context[_]): Unit = {
//    try{
//      for(value <-values) {
//
//        insertStmtStu.setString(1, value.school_id)
//        insertStmtStu.setString(2, value.school_name)
//        insertStmtStu.setString(3, value.hour)
//        insertStmtStu.setString(4, value.time)
//        insertStmtStu.setInt(5, 1)
//        insertStmtStu.setInt(6, value.userType.toInt)
//        insertStmtStu.setString(7, value.time.split(" ")(0))
//        insertStmtStu.setString(8, value.time)
//        insertStmtStu.addBatch()
//
//        //互动任务
//        if (value.request.contains("/selector/tasks/")&&value.appName=="com.xh.smartclassstu") {
//          status = "提交课堂任务"
//        }
//        if (value.request.contains("/completion/tasks/")&&value.appName=="com.xh.smartclassstu") {
//          status = "提交课堂任务"
//        }
//        if (value.request.contains("/question/tasks/")&&value.appName=="com.xh.smartclassstu") {
//          status = "提交课堂任务"
//        }
//        if (value.request.contains("/create/task")&&value.appName=="com.xh.smartclasstch") {
//          status = "布置课堂任务"
//        }
//        //
//        if (value.request.contains("/api/v3/pub/t/works") && value.method == "POST"&& value.appName=="com.xh.acldtch") {
//          status = "布置一份作业"
//        }
//        if (value.request.contains("/api/v4/pub/s/studentWorks") && value.method == "PUT"&& value.appName=="com.xh.acldstu") {
//          status = "上交一份作业"
//        }
//        if (value.request.contains("/v1/s/scanWorks/works") && value.method == "POST" && value.appName=="com.xh.acldtch") {
//          status = "布置一份作业"
//        }
//        if (value.request.contains("/v1/s/scanWorks/users") && value.method == "PUT"&& value.appName=="com.xh.acldstu") {
//          status = "上交一份作业"
//        }
//        if (value.request.contains("/quickAnswer/startQuickAnswer")&&value.appName=="com.xh.smartclasstch") {
//          status = "发起课堂互动"
//        }
//        if (value.request.contains("/quickwriter")&&value.appName=="com.xh.smartclasstch") {
//          status = "发起课堂互动"
//        }
//        if (value.request.contains("/questionnaire/create")&&value.appName=="com.xh.smartclasstch") {
//          status = "发起课堂互动"
//        }
//        if (value.request.contains("/quickAnswer/sendQuickAnswer")&&value.appName=="com.xh.smartclassstu") {
//          status = "响应课堂互动"
//        }
//        if (value.request.contains("quickwriters/")&&value.appName=="com.xh.smartclassstu") {
//          status = "响应课堂互动"
//        }
//        if (value.request.contains("/questionnaire/report")&&value.appName=="com.xh.smartclassstu") {
//          status = "响应课堂互动"
//        }
//        if (status != "") {
//          insertStmt.setString(1, value.userid)
//
//          insertStmt.setString(2, value.user_name)
//          insertStmt.setString(3, status)
//          insertStmt.setString(4, value.time)
//          insertStmt.setString(5, value.school_name)
//          insertStmt.setString(6, value.school_id)
//          insertStmt.setString(7, value.userType)
//          insertStmt.addBatch()
//        }
//        status=""
//      }
//
//      val count1 = insertStmtStu.executeBatch //批量后执行
//      val count2 = insertStmt.executeBatch //批量后执行
//
//      conn.commit
//
//      //System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")
//      //System.out.println("任务成功了插入了了" + count2.length + "行数据")
//
//    }catch {
//      case e: Exception => {
//        log.error("数据异常：%s, \\r\\n %s".format(e, values))
//      }
//    }
//  }
//
//
//  // 关闭时做清理工作
//  override def close(): Unit = {
//    try {
//      insertStmtStu.close()
//      insertStmt.close()
//      //updateStmt.close()
//      conn.close()
//      // println("云mysql关闭成功")
//    } catch {
//      case e: Exception => {
//        println("云mysql关闭失败")
//      }
//    }
//
//  }
//
//  def getConnection(dataSource: BasicDataSource):Connection= {
//    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
//    //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
//    dataSource.setUrl(Url) //test为数据库名
//
//    dataSource.setUsername(User) //数据库用户名
//
//    dataSource.setPassword(Password) //数据库密码
//
//    //设置连接池的一些参数
//    dataSource.setInitialSize(10)
//    dataSource.setMaxTotal(1004)
//    dataSource.setMinIdle(10)
//    var con: Connection = null
//    try {
//      con =dataSource.getConnection
//      con
//    } catch {
//      case e: Exception =>
//        System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage)
//        con
//    }
//
//  }
//
//
//
//}