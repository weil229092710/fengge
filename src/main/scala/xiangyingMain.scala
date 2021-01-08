import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xuehai.utils.{Constants, MysqlUtils, MysqlUtils2, Utils}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



object xiangyingMain extends Constants {

  def main(args: Array[String]) {
    xiangyingmain()
  }

  def xiangyingmain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.of(1, TimeUnit.MINUTES))) //设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次


//    val quUserInfoSql = "select user_id,user_type from fact_user_info"
//    var emptyMap = new mutable.HashMap[Int, Int]()
//    val results: ResultSet = MysqlUtils.select(quUserInfoSql)
//    while (results.next()) {
//      val user_id = results.getInt(1)
//      val user_type = results.getInt(2)
//      emptyMap += (user_id -> user_type)
//    }




    // 用相对路径定义数据源
    //val resource = getClass.getResource("/online.txt")
    //val dataStream = env.readTextFile(resource.getPath)
      val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](xaingyingtopic, new SimpleStringSchema(), props)
      env.addSource(kafkaConsumer)
      .map(x => {
        try {
          //println(x)
          val json = JSON.parseObject(x)

          val obj: JSONObject = JSON.parseObject("{}")
          val sql_type = json.getString("type")
          val table = json.getString("table")
          obj.put("type", sql_type)
          obj.put("table", table)
          //  val data=JSON.parseArray(json.getString("data")).get(0).asInstanceOf[JSONObject]
          val array: JSONArray = JSON.parseArray(json.getString("data"))
          val buffer: ArrayBuffer[JSONObject] = new ArrayBuffer[JSONObject]()

          for (a <- 0 to array.size() - 1) {
            val data = array.get(a).asInstanceOf[JSONObject]

            if (table.contains("immessage")) {
              val from_id = data.getInteger("from_id")
              val created_date = data.getLong("created_date")
              obj.put("created_date",created_date)
              val upTime=Utils.str2hour(created_date)
              obj.put("Time",upTime)
              //val user_type = emptyMap.get(from_id)
              //if(user_type==None) obj.put("user_type", 99)
              //else  obj.put("user_type", user_type.get)
              obj.put("userid", from_id)
              var cur_mill=System.currentTimeMillis()
              obj.put("cur_mill", cur_mill)
              buffer += obj
            }
            if (table == "imgroup") {
              val id = data.getInteger("id")
              val type1 = data.getString("type")
              val used_count = data.getString("user_count")
              obj.put("id",id)
              obj.put("type",type1)
              obj.put("used_count",used_count)
              val created_date = data.getLong("created_date")
              obj.put("created_date",created_date)
              val upTime=Utils.str2hour(created_date)
              obj.put("Time",upTime)
              var cur_mill=System.currentTimeMillis()
              obj.put("cur_mill", cur_mill)
              buffer += obj
            }
            if (table.contains("imgroup_message")) {
              val group_id = data.getInteger("group_id")
              obj.put("group_id",group_id)
              val user_id = data.getInteger("user_id")
              obj.put("user_id",user_id)
              val created_date = data.getLong("created_date")
              obj.put("created_date",created_date)
              val upTime=Utils.str2hour(created_date)
              obj.put("Time",upTime)
              val cur_mill=System.currentTimeMillis()
              obj.put("cur_mill", cur_mill)
              buffer += obj
            }
          }
          buffer

        }
        catch {
          case e: Exception => {

            println(("数据异常：%s, \\r\\n %s".format(e, x)))

            null
          }
        }
      })
      //.filter(_.size>2)

      .addSink(new Sink4())

    //.print()

    env.execute("fengge_xiangying_2")
  }

}
  class Sink4() extends RichSinkFunction[ArrayBuffer[JSONObject]] with Constants {
    // 定义sql连接、预编译器
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var insertGroup: PreparedStatement = _

    var result: ResultSet = null
    var updateStmt: PreparedStatement = _
    var insertGroupCount: PreparedStatement = _
    var insertGroup1Count: PreparedStatement = _

    var status = ""
    val Map = new mutable.HashMap[Int, String]()

    import org.apache.commons.dbcp2.BasicDataSource

    var dataSource: BasicDataSource = null

    val arr = ArrayBuffer[String]()
    var student_info = ""
    var useridString = ""
    var table = ""
    val emptyMap = new mutable.HashMap[Int,String]()
    var empty = new mutable.HashMap[Int, String]()
    var user_count=0
    var type0=0
     var groupStr=""


    var  user_id =0
    var school_id=0
    var user_name=""
    var user_type=0
    var school_name=""
    var  countyname=""
    var  province=""
    var  city_name=""

   // truncate()


    //
//    def truncate() ={
//      val sql1="truncate table all_imgroup_copy"
//
//      try{
//        MysqlUtils.update(sql1)
//
//      }catch {
//        case e: Exception => {
//          LOG.error("清空最近7天信息表出错！"+e.printStackTrace())
//        }
//      }
//    }





    // 初始化，创建连接和预编译语句
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      try {
        import org.apache.commons.dbcp2.BasicDataSource
        dataSource = new BasicDataSource
        conn = getConnection(dataSource)
        //conn.setAutoCommit(false) // 开始事务
        //学生私聊
        insertStmt = conn.prepareStatement("INSERT INTO all_xiangying_count (num,\n  uptime,\n  cur_day,\n  cur_hour\n,school_id,school_name,province,city,county,type)\nVALUES\n\t(?,?,?, ?,?,?,?, ?,?,?) ON DUPLICATE KEY UPDATE num = num + 1,uptime=?")
        //群信息
        insertGroup = conn.prepareStatement("INSERT INTO all_imgroup_copy(group_id,type0,user_count,uptime) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE type0 = ?,user_count =?,uptime=?")
        //教师群发
        insertGroupCount = conn.prepareStatement("INSERT INTO all_xiangying_count(num,uptime,cur_day,cur_hour,school_id,school_name,province,city,county,type) VALUES(?,?,?,?,?,?,?, ?,?, ?) ON DUPLICATE KEY UPDATE num = num + ?,uptime=?")
        //教师私聊
        insertGroup1Count = conn.prepareStatement("INSERT INTO all_xiangying_count(num,uptime,cur_day,cur_hour,school_id,school_name,province,city,county,type) VALUES(?,?,?,?,?,?,?, ?,?, ?) ON DUPLICATE KEY UPDATE num = num + 1,uptime=?")


        //将数据放在内存中
        val quUserInfoSql1 = "select group_id,type0,user_count from all_imgroup_copy"
        val results1: ResultSet = MysqlUtils2.select(quUserInfoSql1)
        while (results1.next()) {
          val id = results1.getInt(1)
          val type0 = results1.getInt(2)
          val user_count = results1.getInt(3)
          val str: String = type0+"#"+user_count
          empty += (id ->str)
        }

        val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \nxh_user_service.XHSys_User a\nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"

        val results: ResultSet = MysqlUtils.select(quUserInfoSql)
        while (results.next()) {

          user_id =results.getInt(1)
          school_id=results.getInt(2)
          user_name=results.getString(3)
          user_type=results.getInt(4)
          school_name=results.getString(5)
          countyname=results.getString(6)
          province=results.getString(7)
          city_name=results.getString(8)
          val info=user_id + "#" + school_id + "#" + school_name + "#" + province+"#" + city_name+"#" + user_type+"#" + countyname
          emptyMap += (user_id -> info)
        }



      }
      catch {
        case e: Exception => {
          println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"相应云mysql连接失败"+e)
        }
      }
    }

    // 调用连接，执行sql
    override def invoke(valuess: ArrayBuffer[JSONObject], context: SinkFunction.Context[_]): Unit = {
      try {
        for (values <- valuess) {
          table = values.getString("table")

          if (table.contains("immessage")) {
            val userid = values.getInteger("userid")


            val userinfo=emptyMap.getOrElse(userid,"信息错误")
            if(userinfo!="信息错误") {
              user_id = userinfo.split("#")(0).toInt
              school_id = userinfo.split("#")(1).toInt
              school_name = userinfo.split("#")(2)
              province = userinfo.split("#")(3)
              city_name = userinfo.split("#")(4)
              user_type = userinfo.split("#")(5).toInt
              countyname= userinfo.split("#")(6)
            }
            if(userinfo=="信息错误"){

              val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \n(select * from xh_user_service.XHSys_User where  iUserId="+userid+") a \nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"

              val results: ResultSet = MysqlUtils.select(quUserInfoSql)
              while (results.next()) {
                user_id =results.getInt(1)
                school_id=results.getInt(2)
                //user_name=results.getString(3)
                user_type=results.getInt(4)
                school_name=results.getString(5)
                countyname=results.getString(6)
                province=results.getString(7)
                city_name=results.getString(8)
              }
            }
            if (user_type == 4) //用户类型 4教师1学生
            {
              insertGroup1Count.setInt(1, 1)
              insertGroup1Count.setString(2, values.getString("Time"))
              insertGroup1Count.setString(3, values.getString("Time").split(" ")(0))
              insertGroup1Count.setInt(4, values.getString("Time").split(" ")(1).split(":")(0).toInt)


              insertGroup1Count.setInt(5, school_id)
              insertGroup1Count.setString(6, school_name)
              insertGroup1Count.setString(7, province)
              insertGroup1Count.setString(8, city_name)
              insertGroup1Count.setString(9, countyname)
              insertGroup1Count.setInt(10, 4)
              insertGroup1Count.setString(11, values.getString("Time"))
              insertGroup1Count.addBatch()
            }
            if (user_type == 1) //用户类型 1学生 4教师
            {
              insertStmt.setInt(1, 1)
              insertStmt.setString(2, values.getString("Time"))
              insertStmt.setString(3, values.getString("Time").split(" ")(0))
              insertStmt.setInt(4, values.getString("Time").split(" ")(1).split(":")(0).toInt)
              insertStmt.setInt(5, school_id)
              insertStmt.setString(6, school_name)
              insertStmt.setString(7, province)
              insertStmt.setString(8, city_name)
              insertStmt.setString(9, countyname)
              insertStmt.setInt(10, 1)
              insertStmt.setString(11, values.getString("Time"))
              insertStmt.addBatch()
            }
          }
          if (table == "imgroup") {

             if(empty.get(values.getInteger("id"))==None){

            val type0 = values.getInteger("type")
             val user_count = values.getInteger("used_count")
               val id = values.getInteger("id").toInt
               insertGroup.setInt(1, id)
               insertGroup.setInt(2, type0)
               insertGroup.setInt(3, user_count)
               //val str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
               val upTime=Utils.str2hour(values.getLong("created_date"))

               insertGroup.setString(4, upTime)
               insertGroup.setInt(5, type0)
               insertGroup.setInt(6, user_count)
               insertGroup.setString(7, upTime)
               insertGroup.addBatch()
          }
          }
          if (table.contains("imgroup_message")) {
            val userid = values.getInteger("user_id")

            val userinfo=emptyMap.getOrElse(userid,"信息错误")
            if(userinfo!="信息错误") {
              user_id = userinfo.split("#")(0).toInt
              school_id = userinfo.split("#")(1).toInt
              school_name = userinfo.split("#")(2)
              province = userinfo.split("#")(3)
              city_name = userinfo.split("#")(4)
              user_type = userinfo.split("#")(5).toInt
              countyname= userinfo.split("#")(6)
            }
            if(userinfo=="信息错误"){

              val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \n(select * from xh_user_service.XHSys_User where  iUserId="+user_id+") a \nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"

              val results: ResultSet = MysqlUtils.select(quUserInfoSql)
              while (results.next()) {
                user_id =results.getInt(1)
                school_id=results.getInt(2)
                //user_name=results.getString(3)
                //user_type=results.getInt(4)
                school_name=results.getString(5)
                countyname=results.getString(6)
                province=results.getString(7)
                city_name=results.getString(8)
              }
            }
            var group_id = values.getInteger("group_id")
            if(empty.get(values.getInteger("group_id"))==Some){
              groupStr=empty.get(values.getInteger("group_id")).get
              type0=groupStr.split("#")(0).toInt
              user_count=groupStr.split("#")(1).toInt
            }
            else {
              val quInfoSql = "select type0,user_count from all_imgroup_copy where group_id =" + values.getInteger("group_id");
              val results: ResultSet = MysqlUtils2.select(quInfoSql)
              while (results.next()) {
                type0 = results.getInt(1)
                user_count = results.getInt(2)
              }
            }

            if (type0==1) //
            {
              insertGroupCount.setInt(1, user_count-1)
              insertGroupCount.setString(2, values.getString("Time"))
              insertGroupCount.setString(3, values.getString("Time").split(" ")(0))
              insertGroupCount.setInt(4, values.getString("Time").split(" ")(1).split(":")(0).toInt)
              insertGroupCount.setInt(5, school_id)
              insertGroupCount.setString(6, school_name)
              insertGroupCount.setString(7, province)
              insertGroupCount.setString(8, city_name)
              insertGroupCount.setString(9, countyname)
              insertGroupCount.setInt(10, 4)
              insertGroupCount.setInt(11, user_count-1)
              insertGroupCount.setString(12, values.getString("Time"))
              insertGroupCount.addBatch()
            }
          }

        }




              val count1 = insertStmt.executeBatch //批量后执行
              val count2=insertGroup.executeBatch()//
              val count3= insertGroupCount.executeBatch()
              val count4= insertGroup1Count.executeBatch()
        //
        //
//              System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")
//              System.out.println("任务成功了插入了了" + count2.length + "行数据")
//        System.out.println("接口访问量成功了插入了了" + count3.length + "行数据")
//        System.out.println("任务成功了插入了了" + count4.length + "行数据")

        //conn.commit
      } catch {
        case e: Exception => {
          println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"响应数据错误" +e+ valuess)
        }
      }
    }


    // 关闭时做清理工作
    override def close(): Unit = {
      try {

        insertGroupCount.close()
        insertGroup1Count.close()

        insertStmt.close()
        insertGroup.close()
        conn.close()
        //println("云mysql关闭成功")
      } catch {
        case e: Exception => {
           println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"云mysql关闭失败"+e)
        }
      }

    }

    def getConnection(dataSource: BasicDataSource): Connection = {
      dataSource.setDriverClassName("com.mysql.jdbc.Driver")

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

