//import java.sql.{Connection, PreparedStatement, ResultSet}
//import java.text.SimpleDateFormat
//import java.util
//import java.util.Locale
//
//import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
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
//import com.xuehai.utils.{Constants, MysqlUtils, Utils}
//import org.apache.flink.api.common.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//
//import scala.collection.mutable
//import scala.collection.mutable.{ArrayBuffer, ListBuffer}
//
//
//
//object aa extends Constants{
//
//  def main(args: Array[String]) {
//    taskmain3()
//  }
//  def taskmain3(): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
//    val obj: JSONObject = JSON.parseObject("{}")
//
//
//
//    // 用相对路径定义数据源
//    val resource = getClass.getResource("/yunketang.txt")
//    val dataStream = env.readTextFile(resource.getPath)
//      // val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](mongotopic, new SimpleStringSchema(), props)
//      // env.addSource(kafkaConsumer)
//
//      .map(x => {
//        try{
//          println(x)
//          val json=	JSON.parseObject(x)
//
//
//          val message=json.getString("message")
//          val data=message.split("\\|")
//          val room_id=data(3)
//          val login_type=data(6)
//          val updateTime=data(7).toLong
//          val upTime=Utils.str2hour(updateTime)
//          obj.put("updateTime",updateTime)
//          obj.put("upTime",upTime)
//          obj.put("room_id",room_id)
//          obj.put("login_type",login_type)
//          obj
//        }
//        catch {
//          case e: Exception => {
//            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
//            log.error("峰哥实时数据异常：%s, \\r\\n %s".format(e, x))
//            println(e)
//            JSON.parseObject("{}")
//          }
//        }
//      })
//      .filter(_.getLong("updateTime")!=null)
//      .assignAscendingTimestamps(_.getLong("updateTime")*1000)
//      .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(4000))
//
//      .apply(new ByWindow1())
//      .addSink(new  MySqlSink2())
//
//    //.print()
//
//    env.execute(jobName)
//  }
//}
//
//class ByWindow1() extends AllWindowFunction[JSONObject, Iterable[JSONObject], TimeWindow]{
//  override def apply(window: TimeWindow, input: Iterable[JSONObject], out: Collector[Iterable[JSONObject]]): Unit = {
//
//
//
//    if(input.nonEmpty) {
//      System.out.println("1 秒内收集到 接口的条数是：" + input.size)
//      out.collect(input)
//    }
//  }
//}
//
//
//class MySqlSink2() extends RichSinkFunction[Iterable[JSONObject]] with Constants {
//  // 定义sql连接、预编译器
//  var conn: Connection = _
//  var insertStmt: PreparedStatement = _
//  var insertAutoCorrect: PreparedStatement = _
//  var insertTiZhou: PreparedStatement = _
//
//  var result: ResultSet = null
//
//  var table= ""
//  var idSet = Set[String]()
//  var online=1
//  var max_num=1
//
//
//
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
//      insertStmt = conn.prepareStatement("UPDATE all_yunketang_real set online_count=online_count+?,max_count=max_count+? where room_id=?")
//
//    }
//    catch {
//      case e: Exception => {
//        println("云mysql连接失败")
//      }
//    }
//  }
//
//
//
//  // 调用连接，执行sql
//  override def invoke(values: Iterable[JSONObject], context: SinkFunction.Context[_]): Unit = {
//    // try{
//    for(value <-values) {
//      // 定义一个scala set，用于保存所有的数据userId并去重
//
//      // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
//
//      //idSet += value.getString("room_id")
//      val class_id=value.getString("room_id")
//      val login_type=value.getInteger("login_type")
//      if(login_type!=1){
//        online=0-1
//        max_num=0
//      }
//      insertStmt.setInt(1,online)
//      insertStmt.setInt(2,max_num)
//      insertStmt.setString(3,class_id)
//      insertStmt.addBatch()
//      online=1
//      max_num=1
//    }
//
//    //      val count1 = insertAutoCorrect.executeBatch //批量后执行
//    val count2 = insertStmt.executeBatch //批量后执行
//    //      val count3 = insertTiZhou.executeBatch //批量后执行
//    //    conn.commit
//
//    //System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")
//    System.out.println("任务成功了插入了了" + count2.length + "行数据")
//
//    //    }catch {
//    //      case e: Exception => {
//    //        log.error("数据异常：%s, \\r\\n %s".format(e, values))
//    //        print("数据异常"+e + values)
//    //      }
//    //    }
//  }
//
//
//  // 关闭时做清理工作
//  override def close(): Unit = {
//    try {
//      //insertAutoCorrect.close()
//      insertStmt.close()
//
//      conn.close()
//      // println("云mysql关闭成功")
//    } catch {
//      case e: Exception => {
//        // println("云mysql关闭失败"+e)
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