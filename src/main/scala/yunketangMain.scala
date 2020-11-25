import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
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

import com.xuehai.utils.{Constants, MysqlUtils, MysqlUtils2, Utils}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}



object yunketangMain extends Constants{

  def main(args: Array[String]) {
    taskmain3()
  }
  def taskmain3(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //kafka数据去掉
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //本地文件打开

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
    val obj: JSONObject = JSON.parseObject("{}")



    // 用相对路径定义数据源
  //val resource = getClass.getResource("/yunketang1.txt")
   //val dataStream = env.readTextFile(resource.getPath)
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](chanaltopic, new SimpleStringSchema(), props)
    env.addSource(kafkaConsumer)
            .filter(x=>{
              try {
               // println(x)
                val json = JSON.parseObject(x)
                val message=json.getString("message")
                val data=message.split("\\|")
                data.size>3
              }catch {
                case e: Exception => {
                  //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
                  log.error("过滤时异常：%s, \\r\\n %s".format(e, x))
                  println(("过滤时异常：" + x))
                  false
                }
              }
            })

     .map(x => {
         try{
            //println(x)
           val json=	JSON.parseObject(x)


           val message=json.getString("message")
           val data=message.split("\\|")
           val room_id=data(3)
           val login_type=data(6)
           val updateTime=data(7).toLong
           var upTime=System.currentTimeMillis()
           //val upTime=Utils.str2hour(updateTime)
           obj.put("updateTime",updateTime)
           obj.put("upTime",upTime)
           obj.put("room_id",room_id)
           obj.put("login_type",login_type)
           obj
         }
         catch {
           case e: Exception => {
             //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
             log.error("峰哥实时数据异常：%s, \\r\\n %s".format(e, x))
             println("允收"+e)
             JSON.parseObject("{}")
           }
         }
       })
      // .filter(_.getLong("updateTime")!=null)
       .assignAscendingTimestamps(_.getLong("updateTime")*1000)
       .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(40))

      .apply(new ByWindow2())
      .addSink(new MySqlSink7())

    // .print()

    env.execute("yunketang")
  }





}

class ByWindow2() extends AllWindowFunction[JSONObject, Iterable[JSONObject], TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[JSONObject], out: Collector[Iterable[JSONObject]]): Unit = {



    if(input.nonEmpty) {
           System.out.println("通道内数据1秒内收集到 接口的条数是：" + input.size)
          out.collect(input)
        }
  }
}


class MySqlSink7() extends RichSinkFunction[Iterable[JSONObject]] with Constants {
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var insertAutoCorrect: PreparedStatement = _
  var updateMax: PreparedStatement = _
  var updateStatusInfo: PreparedStatement = _
  var updateStatus1Info: PreparedStatement = _
  var insertClassHistory: PreparedStatement = _

  var result: ResultSet = null

  var table= ""
  var idSet = Set[String]()
  var online=1
  var max_num=1
  var status1to2="0"
  var status2to3="0"
  var insertTiZhou: PreparedStatement = _
  var aa=0



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
      conn.setAutoCommit(false) // 开始事务  INSERT INTO all_yunketang_hitory (class_id, class_name, subject_id, subject_name, uptime, cur_hour, cur_day) VALUES (?,?,?,?,?,?,?);

      insertStmt = conn.prepareStatement("UPDATE all_yunketang_real set online_count=case when online_count+? <0 then 0 else online_count+? end,max_count=max_count+? where room_id=?")
      updateMax = conn.prepareStatement("UPDATE all_yunketang_real set max_count=pre_count where room_id=?")
      updateStatusInfo = conn.prepareStatement("UPDATE all_yunketang_real set `status`=?,statusUptime1to2=?,statusUphour1to2=? where room_id=?")
      updateStatus1Info = conn.prepareStatement("UPDATE all_yunketang_real set `status`=?,max_count=0,statusUptime2to3=?,statusUphour2to3=? where room_id=?")
      insertClassHistory = conn.prepareStatement("INSERT INTO all_yunketang_history (class_id, class_name, subject_id, subject_name, uptime, cur_hour, cur_day,count,school_id,school_name,province,city) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")

    }
    catch {
      case e: Exception => {
        println("云mysql连接失败"+e)
      }
    }
  }



  // 调用连接，执行sql
  override def invoke(values: Iterable[JSONObject], context: SinkFunction.Context[_]): Unit = {
    try{
      for(value <-values) {
        // 定义一个scala set，用于保存所有的数据userId并去重

        // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
        idSet += value.getString("room_id")
        val class_id=value.getString("room_id")
        val login_type=value.getInteger("login_type")
        if(login_type!=1){
          online=0-1
          max_num=0
        }
        insertStmt.setInt(1,online)
        insertStmt.setInt(2,online)
        insertStmt.setInt(3,max_num)
        insertStmt.setString(4,class_id)
        insertStmt.addBatch()

        online=1
        max_num=1
      }

      val count1 = insertStmt.executeBatch //批量后执行
      conn.commit()

      for(classid <-idSet) {
        //查询课堂的预设人数
        val qusubjectInfo = "select room_id,pre_count,online_count,status,max_count ,room_name,subject_id,subject_name,school_id,school_name,province,city from all_yunketang_real where room_id= '"+classid+"'"
        val results1: ResultSet = MysqlUtils2.select(qusubjectInfo)
        while (results1.next()) {
          val room_id = results1.getString(1)
          val pre_count = results1.getInt(2)
          val online_count = results1.getInt(3)
          var status =results1.getInt(4)
          val max_num1 =results1.getInt(5)
          val class_name =results1.getString(6)
          val subject_id =results1.getInt(7)
          val subject_name =results1.getString(8)
          val school_id =results1.getInt(9)
          val school_name =results1.getString(10)
          val province =results1.getString(11)
          val city =results1.getString(12)


          if(max_num1>pre_count){
            //将最大数据改为预设人数
            updateMax.setString(1,classid)
            updateMax.addBatch()
          }
          if(status==1&&online_count>=(pre_count*0.3).toInt){
            status=2
            status1to2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
            //将未上课状态改为上课状态
            //并将状态改变时间导入结果   value.getString("upTime").split(" ")(1).split(":")(0).toInt
            updateStatusInfo.setInt(1,status)
            updateStatusInfo.setString(2,status1to2)
            updateStatusInfo.setInt(3,status1to2.split(" ")(1).split(":")(0).toInt)
            updateStatusInfo.setString(4,room_id)
            updateStatusInfo.addBatch()
          }

          if(status==2&&online_count<pre_count*0.3){
            status=1
            status2to3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
            //1，将状态改为未上课状态，并将状态改变时间导入结果
            //2.同时将最大结果插入到已经上过课的新表中
            updateStatus1Info.setInt(1,status)
            updateStatus1Info.setString(2,status2to3)
            updateStatus1Info.setInt(3,status2to3.split(" ")(1).split(":")(0).toInt)
            updateStatus1Info.setString(4,room_id)
             updateStatus1Info.addBatch()
            //将已经上过课的课堂和人数插入到表中
            insertClassHistory.setString(1,classid)
            insertClassHistory.setString(2,class_name)
            insertClassHistory.setInt(3,subject_id)
            insertClassHistory.setString(4,subject_name)
            insertClassHistory.setString(5, status2to3)
            insertClassHistory.setInt(6, status2to3.split(" ")(1).split(":")(0).toInt)
            insertClassHistory.setString(7, status2to3.split(" ")(0))
            //维度信息
            if(max_num1<pre_count) aa=max_num
            else aa=pre_count
            insertClassHistory.setInt(8,aa)
            insertClassHistory.setInt(9,school_id)
            insertClassHistory.setString(10,school_name)
            insertClassHistory.setString(11,province)
            insertClassHistory.setString(12, city)
            insertClassHistory.addBatch()
          }
          status1to2="0"
          status2to3="0"

        }

      }

      val count2 = updateMax.executeBatch //批量后执行

      val count3 = updateStatusInfo.executeBatch //批量后执行

      val count4 = updateStatus1Info.executeBatch //批量后执行
      val count5 = insertClassHistory.executeBatch //批量后执行

      conn.commit

      System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")
      System.out.println("任务成功了插入了了" + count2.length + "行数据")
      System.out.println("接口访问量成功了插入了了" + count3.length + "行数据")
      System.out.println("任务成功了插入了了" + count4.length + "行数据")
      System.out.println("接口访问量成功了插入了了" + count5.length + "行数据")
    }catch {
      case e: Exception => {
        log.error("数据异常：%s, \\r\\n %s".format(e, values))
        print("数据异常"+e + values)
      }
    }
  }


  // 关闭时做清理工作
  override def close(): Unit = {
    try {

      insertStmt.close()
      updateMax.close()
      updateStatusInfo.close()
      updateStatus1Info.close()
      insertClassHistory.close()

      conn.close()
      // println("云mysql关闭成功")
    } catch {
      case e: Exception => {
        println("云mysql关闭失败"+e)
      }
    }

  }

  def getConnection(dataSource: BasicDataSource):Connection= {
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
      con =dataSource.getConnection
      con
    } catch {
      case e: Exception =>
        System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage)
        con
    }

  }



}