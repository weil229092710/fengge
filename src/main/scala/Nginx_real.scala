import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.util.concurrent.TimeUnit

import Nginx_real.task
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Nginx_real extends Constants_nginx {

  case class task(userid: String,
                  user_name: String,
                  school_id: String,
                  school_name: String,
                  city: String,
                  hour: String,
                  time: String,
                  userType: String,
                  request: String,
                  appName: String,
                  method: String,
                  timestap: Long,
                  province: String,
                  city_name: String
                 )
  def main(args: Array[String]) {
    taskmain()
  }

  def taskmain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(20 * 1000)//开启checkPoint，并且每分钟做一次checkPoint保存
    //env.setStateBackend(new FsStateBackend(checkPointPath))
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.of(1, TimeUnit.MINUTES))) //设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
    val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \nxh_user_service.XHSys_User a\nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"
    var emptyMap = new mutable.HashMap[Int, JSON]()
    var emMap = new mutable.HashMap[Long,Int]()
    val results: ResultSet = MysqlUtils.select(quUserInfoSql)
    var aa=1

    while (results.next()) {
      val json = JSON.parseObject("{}")
      val user_id = results.getInt(1)
      json.put("user_id", results.getInt(1))
      json.put("school_id", results.getString(2))
      json.put("user_name", results.getString(3))
      json.put("user_type", results.getInt(4))
      json.put("school_name", results.getString(5))
      json.put("city", results.getString(6))
      json.put("province", results.getString(7))
      json.put("city_name", results.getString(8))

      emptyMap += (user_id -> json)
    }
    // 用相对路径定义数据源
    //val resource = getClass.getResource("/hello.txt")
    //val dataStream = env.readTextFile(resource.getPath)
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
    env.addSource(kafkaConsumer)
      .filter(x => {
        try {
          //println(x)
          val json = JSON.parseObject(x)
          val userid = json.getString("UserId")
          userid != "-"
        } catch {
          case e: Exception => {
            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
            log.error("数据异常：%s, \\r\\n %s".format(e, x))
            false
          }
        }
      })

      .map(x => {
        try {
         // println("过滤后"+x)

          val json = JSON.parseObject(x)
          val userid = json.getString("UserId")
          val request = json.getString("request")
          val appName = json.getString("appName")
          val method = json.getString("method")

          val school_id = JSON.parseObject(emptyMap(userid.toInt).toString).get("school_id").toString
          val school_name = JSON.parseObject(emptyMap(userid.toInt).toString).get("school_name").toString
          val scountyName = JSON.parseObject(emptyMap(userid.toInt).toString).get("city").toString
          val userType = JSON.parseObject(emptyMap(userid.toInt).toString).get("user_type").toString
          val userName = JSON.parseObject(emptyMap(userid.toInt).toString).get("user_name").toString
          val province = JSON.parseObject(emptyMap(userid.toInt).toString).get("province").toString
          val cityName = JSON.parseObject(emptyMap(userid.toInt).toString).get("city_name").toString

//          val time_local = json.getString("time_local")
//          //val time=Utils.toLoclTime(time_local)
//          val sdf1 = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
//          val date = sdf1.parse(time_local)
//          val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          //val time = sdf2.format(date)
          val time=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
          val hour = time.split(" ")(1).split(":")(0).toInt


          val timeStamp = System.currentTimeMillis();

          task(userid, userName, school_id, school_name, scountyName, hour.toString, time, userType, request, appName, method, timeStamp, province, cityName)
        }
        catch {
          case e: Exception => {
            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
            log.error("数据异常：%s, \\r\\n %s".format(e, x))
            task("", "", "", "", "", "", "", "", "", "", "", 0, "", "")
          }
        }
      })
      .filter(_.userType != "")

      //.filter(_.school_name != "")

      .assignAscendingTimestamps(_.timestap * 2000)

      .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(2000))

      .apply(new ByWindow4())
      .addSink(new MySqlSink22())

    // .print()

    env.execute("nginx_real_all")
  }
}

class ByWindow4() extends AllWindowFunction[task, Iterable[task], TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[task], out: Collector[Iterable[task]]): Unit = {


    if (input.nonEmpty) {
       System.out.println("1 秒内收集到 接口的条数是：" + input.size)
      out.collect(input)
    }



  }

}




case class userInfo(userid: Int,
                    user_name: String,
                    school_id: String,
                    school_name: String,
                    city: String,
                    userType: Int
                   )


case class active(schoolId: String, school_name: String, time: String, userType: String, count: Int)


class MySqlSink22() extends RichSinkFunction[Iterable[task]] with Constants_nginx {
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var insertStmtStu: PreparedStatement = _
  var insertmeiwen: PreparedStatement = _
  var result: ResultSet = null
  var updateStmt: PreparedStatement = _
  var insertactive: PreparedStatement = _
  var insertactive1: PreparedStatement = _
  var status = ""
  var areadunc = 0
  var aartunc = 0
  var submit = 0
  val collectMap = new mutable.HashMap[String, Int]()

  var inserthudong: PreparedStatement = _

  val Map = new mutable.HashMap[String, String]()
  var aa=0
  var hudong=0

  import org.apache.commons.dbcp2.BasicDataSource

  var dataSource: BasicDataSource = null

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    try {
      //Class.forName("com.mysql.jdbc.Driver")
      //conn = DriverManager.getConnection(Url, User, Password)
      import org.apache.commons.dbcp2.BasicDataSource

      dataSource = new BasicDataSource
      conn = getConnection(dataSource)
      //conn.setAutoCommit(false) // 开始事务  INSERT INTO active_time (num) VALUES (?);


      insertStmt = conn.prepareStatement("INSERT INTO all_real_class_copy (user_id, user_name,status,time,school_name,school_id,user_type,province,city,county) VALUES (?,?,?,?,?,?,?,?,?,?)")
      insertStmtStu = conn.prepareStatement("INSERT INTO all_active_num_version2 (school_id, school_name,count,user_type,time,cur_date,province,city,app,cur_hour,county) VALUES (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update count=count+?,time=?")
      //insertmeiwen=conn.prepareStatement("INSERT INTO all_real_meiwen (\n\tmeiwen_num,\n  yuedu_num,\n  yuntingshuo_num,\n  uptime,\n  cur_hour,\n  cur_day\n)\nVALUES\n\t(?,?,?, ?, ?, ?) ON DUPLICATE KEY UPDATE meiwen_num = meiwen_num + ?,yuedu_num = yuedu_num + ?,yuntingshuo_num = yuntingshuo_num + ?,uptime=?")
      inserthudong=conn.prepareStatement("iNSERT INTO all_hudong (\n  userid,\n  province,\n  city,\n\tschool_id,\n  school_name,\n  uptime,\ncur_hour,\ncur_day,county)\nVALUES\n\t(?, ?, ?, ?, ?,?,?,?,?) ON DUPLICATE KEY UPDATE  cur_hour=?")

      //insertactive=conn.prepareStatement("INSERT INTO active_time (num) VALUES (?)")
      insertactive1=conn.prepareStatement("INSERT INTO active_time1 (num,school_id,province,city,county,school_name) VALUES (?,?,?,?,?,?)")

    }
    catch {
      case e: Exception => {
        println("云mysql连接失败"+e)
      }
    }
  }

  // 调用连接，执行sql
  override def invoke(values: Iterable[task], context: SinkFunction.Context[_]): Unit = {
    try {
      for (value <- values) {

        if (collectMap.contains(value.school_id + "*" + value.userType)) { //若map中已经存在
          val maybeInt: Option[Int] = collectMap.get(value.school_id + "*" + value.userType)

          collectMap.put(value.school_id + "*" + value.userType, maybeInt.get + 1)
        }
        else {
          collectMap.put(value.school_id + "*" + value.userType, 1)
          Map.put(value.school_id + "*" + value.userType, value.school_name + "#" + value.time + "#" + value.province + "#" + value.city_name+"#" + value.appName+ "#" + value.city)
        }
        //互动任务
        if (value.request.contains("/selector/tasks/") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "提交课堂任务"
          hudong=1
        }
        if (value.request.contains("/completion/tasks/") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "提交课堂任务"
          hudong=1
        }
        if (value.request.contains("/question/tasks/") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "提交课堂任务"
          hudong=1
        }
        if (value.request.contains("/create/task") && (value.appName == "com.xh.smartclasstch"||value.appName=="com.xh.asctch")) {
          status = "布置课堂任务"
          hudong=1
        }
        //
        if (value.request.contains("/api/v3/pub/t/works") && value.method == "POST" && value.appName == "com.xh.acldtch") {
          status = "布置一份作业"
          hudong=1
        }
        if (value.request.contains("/api/v4/pub/s/studentWorks") && value.method == "PUT" && value.appName == "com.xh.acldstu") {
          status = "上交一份作业"
          hudong=1
        }
        if (value.request.contains("/v1/s/scanWorks/works") && value.method == "POST" && value.appName == "com.xh.acldtch") {
          status = "布置一份作业"
          hudong=1
        }
        if (value.request.contains("/v1/s/scanWorks/users") && value.method == "PUT" && value.appName == "com.xh.acldstu") {
          status = "上交一份作业"
          hudong=1
        }
        if (value.request.contains("/quickAnswer/startQuickAnswer") && (value.appName == "com.xh.smartclasstch"||value.appName=="com.xh.asctch")) {
          status = "发起课堂互动"
          hudong=1
        }
        if (value.request.contains("/quickwriter") && (value.appName == "com.xh.smartclasstch"||value.appName=="com.xh.asctch")) {
          status = "发起课堂互动"
          hudong=1
        }
        if (value.request.contains("/questionnaire/create") && (value.appName == "com.xh.smartclasstch"||value.appName=="com.xh.asctch")) {
          status = "发起课堂互动"
          hudong=1
        }
        if (value.request.contains("/quickAnswer/sendQuickAnswer") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "响应课堂互动"
          hudong=1
        }
        if (value.request.contains("quickwriters/") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "响应课堂互动"
          hudong=1
        }
        if (value.request.contains("/questionnaire/report") && (value.appName == "com.xh.smartclassstu"||value.appName=="com.xh.ascstu")) {
          status = "响应课堂互动"
          hudong=1
        }
        if (status != "") {
          insertStmt.setString(1, value.userid)

          insertStmt.setString(2, value.user_name)
          insertStmt.setString(3, status)
          insertStmt.setString(4, value.time)
          insertStmt.setString(5, value.school_name)
          insertStmt.setString(6, value.school_id)
          insertStmt.setString(7, value.userType)
          insertStmt.setString(8, value.province)
          insertStmt.setString(9, value.city_name)
          insertStmt.setString(10, value.city)
          insertStmt.addBatch()
        }
        if(hudong==1){
          inserthudong.setInt(1, value.userid.toInt)
          inserthudong.setString(2, value.province)
          inserthudong.setString(3,value.city_name )
          inserthudong.setString(4, value.school_id)
          inserthudong.setString(5, value.school_name)
          inserthudong.setString(6, value.time)
          inserthudong.setInt(7, value.time.split(" ")(1).split(":")(0).toInt)
          inserthudong.setString(8, value.time.split(" ")(0))
          inserthudong.setString(9,value.city )
          inserthudong.setInt(10, value.time.split(" ")(1).split(":")(0).toInt)
          inserthudong.addBatch()
        }
        status = ""
        hudong=0
//悦读，美文，云作业
//        //悦读
//        if (value.request.contains("/api/v2/articles") && value.appName == "com.xh.areadunc") {
//          areadunc = 1
//        }
//        //美文
//        if (value.request.contains("/api/v1/article/save/publish") && value.appName == "com.xh.aartunc") {
//          aartunc = 1
//        }
//        if (value.request.contains("/api/v2/mark") && value.appName == "com.xh.aartunc") {
//          aartunc = 1
//        }
//        if (value.request.contains("/api/v1/article/save/cloudMailbox") && value.appName == "com.xh.aartunc") {
//          aartunc = 1
//        }
//        //云听说
//        if (value.request.contains("/api/v1/task/submit/homework")) {
//          submit = 1
//        }
//        if (value.request.contains("/api/v1/task/submit/exam")) {
//          submit = 1
//        }
//
//        if(submit==1||aartunc==1||areadunc==1){
//          insertmeiwen.setInt(1, aartunc)
//          insertmeiwen.setInt(2, areadunc)
//          insertmeiwen.setInt(3, submit)
//          insertmeiwen.setString(4, value.time)
//          insertmeiwen.setString(5, value.hour)
//          insertmeiwen.setString(6, value.time.split(" ")(0))
//          insertmeiwen.setInt(7, aartunc)
//          insertmeiwen.setInt(8, areadunc)
//          insertmeiwen.setInt(9, submit)
//          insertmeiwen.setString(10, value.time)
//          insertmeiwen.addBatch()
//        }
//
//         //清空悦读，美文，云听说的值，避免下一次循环仍然用上一次循环的值
//        areadunc = 0
//        aartunc = 0
//        submit = 0

      }
      for ((key, value) <- Map) {
        //println("key is" + key + " ,value is" + value)
        insertStmtStu.setString(1, key.split("\\*")(0))
        insertStmtStu.setString(2, value.split("#")(0))
        insertStmtStu.setInt(3, collectMap.get(key).get)
        insertStmtStu.setString(4, key.split("\\*")(1))
        insertStmtStu.setString(5, value.split("#")(1))
        insertStmtStu.setString(6, value.split("#")(1).split(" ")(0))
        insertStmtStu.setString(7, value.split("#")(2))
        insertStmtStu.setString(8, value.split("#")(3))
        insertStmtStu.setString(9,appNameMap.getOrElse(value.split("#")(4),"其它") )
        insertStmtStu.setInt(10, value.split("#")(1).split(" ")(1).split(":")(0).toInt)
        insertStmtStu.setString(11, value.split("#")(5))
        insertStmtStu.setInt(12, collectMap.get(key).get * 2)
        insertStmtStu.setString(13, value.split("#")(1))
        //insertactive.setInt(1,collectMap.get(key).get)
        insertactive1.setInt(1,collectMap.get(key).get)
        insertactive1.setInt(2,key.split("\\*")(0).toInt)
        insertactive1.setString(3,value.split("#")(2))
        insertactive1.setString(4,value.split("#")(3))
        insertactive1.setString(5,value.split("#")(5))
        insertactive1.setString(6,value.split("#")(0))
        insertStmtStu.addBatch()
        //insertactive.addBatch()
        insertactive1.addBatch()
      }

      Map.clear()
      collectMap.clear()
      val count1 = insertStmtStu.executeBatch //批量后执行
      val count2 = insertStmt.executeBatch //批量后执行
      //val count3=insertmeiwen.executeBatch()//美文批量执行
      val count4=inserthudong.executeBatch()
     // val count5=insertactive.executeBatch()
      val count6=insertactive1.executeBatch()

      //conn.commit

//      System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")
//      System.out.println("任务成功了插入了了" + count2.length + "行数据")
//      //System.out.println("接口访问量成功了插入了了" + count3.length + "行数据")
//      System.out.println("任务成功了插入了了" + count4.length + "行数据")

    } catch {
      case e: Exception => {
        log.error("数据异常"+e)
        println("异常" + e )
      }
    }
  }


  // 关闭时做清理工作
  override def close(): Unit = {
    try {
      insertStmtStu.close()
       insertStmt.close()
      //insertmeiwen.close()
      inserthudong.close()
      //insertactive.close()
      insertactive1.close()
      conn.close()
      // println("云mysql关闭成功")
    } catch {
      case e: Exception => {
        println("云mysql关闭失败")
      }
    }

  }

  def getConnection(dataSource: BasicDataSource): Connection = {
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
    dataSource.setUrl(Url) //test为数据库名

    dataSource.setUsername(User) //数据库用户名

    dataSource.setPassword(Password) //数据库密码

    //设置连接池的一些参数
    dataSource.setInitialSize(10)
    dataSource.setMaxTotal(1004)
    dataSource.setMinIdle(10)
    var con: Connection = null
    try {
      con = dataSource.getConnection
      con
    } catch {
      case e: Exception =>
        System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage)
        con
    }

  }


}