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

import com.xuehai.utils.{Constants, MysqlUtils, MysqlUtils1, MysqlUtils2, Utils}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}



object AllMain extends Constants{

  def main(args: Array[String]) {
    taskmain()

  }
  def taskmain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
    val obj: JSONObject = JSON.parseObject("{}")
    import scala.collection.mutable.ArrayBuffer
    //val arrays = ArrayBuffer[Int]() //1.不需要关键字new。2.需要指明数据类型。
    var emptyMap = new mutable.HashMap[Int,String]()
    var num=0
    var valid="true"
    var parentQuestionId="0"
    var count=0
    var isHomeWork=0
    var countan=0
    var sum=0

    var countan1=0
    var sum1=0


    // 用相对路径定义数据源
   //val resource = getClass.getResource("/hello.txt")
   //val dataStream = env.readTextFile(resource.getPath)
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](mongotopic, new SimpleStringSchema(), props)
    env.addSource(kafkaConsumer)
//      .filter(x=>{
//        try {
//          //println(x)
//         x.contains("questionList")
//        }catch {
//          case e: Exception => {
//            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
//            log.error("数据异常d：%s, \\r\\n %s".format(e, x))
//            false
//          }
//        }
//      })
       .map(x => {
         try{
            println(x)
           val json=	JSON.parseObject(x)
           obj.clear()
           emptyMap.clear()
           val sql_type=json.getString("op")
           val table=json.getString("ns")
           if(sql_type=="u") {
             obj.put("id", json.getJSONObject("o2").getString("_id"))
           }
           obj.put("sql_type",sql_type)
           obj.put("table",table)
           //  val data=JSON.parseArray(json.getString("data")).get(0).asInstanceOf[JSONObject]
           val array: JSONArray = JSON.parseArray(json.getString("o"))
           //val buffer: ArrayBuffer[JSONObject] = new ArrayBuffer[JSONObject]()
           obj.put("proofreadState",0)
           for( a <- 0 to array.size()-1) {

             val data = array.get(a).asInstanceOf[JSONObject]


             if ((table == "xh_cloudwork_parts.work" || table=="xh_cloudwork_exam.teacherExam")&&sql_type=="i") {
               if (data.getString("Name")=="_id"){
                 val id=data.getString("Value")
                 obj.put("id",id)
               }

               if (data.getString("Name")=="handInNum"){
                 val handInNum=data.getString("Value")
                 obj.put("handInNum",handInNum)
               }


               if (data.getString("Name")=="uptoTime"){

                 val upTime=Utils.str2hour(data.getLong("Value"))
                 obj.put("upto_time",upTime)
               }


               if (data.getString("Name")=="subject"){
                 val subject=data.getString("Value")
                 obj.put("subject",subject)
               }

               if (data.getString("Name")=="teacherId"){
                 val teacherId=data.getString("Value")
                 obj.put("teacherId",teacherId)
               }
               if (data.getString("Name")=="topicNum"){
                 val topicNum=data.getString("Value")
                 obj.put("topicNum",topicNum)
               }
               if (data.getString("Name")=="proofreadState"){
                 val proofreadState=data.getString("Value")
                 obj.put("proofreadState",proofreadState)
               }
               if (data.getString("Name")=="updateTime"){
                 val updateTime=data.getLong("Value")
                 val upTime=Utils.str2hour(updateTime)
                 obj.put("updateTime",updateTime)
                 obj.put("upTime",upTime)
               }
             }
             if ((table == "xh_cloudwork_parts.work" || table=="xh_cloudwork_exam.teacherExam")&&sql_type=="u") {

               if (data.getString("Name")=="$set"){
                 val datax=data.getJSONArray("Value")
                 for( a <- 0 to datax.size()-1) {
                   val datas = datax.get(a).asInstanceOf[JSONObject]
                   if (datas.getString("Name")=="handIn"){
                     val handin=datas.getString("Value")
                     obj.put("handin",handin)
                   }
                 }
               }
               val xx=System.currentTimeMillis()
               val upTime=Utils.str2hour(xx)
               obj.put("updateTime",xx)
               obj.put("upTime",upTime)
             }
             if (table == "xh_cloudwork_parts.studentWork" || table=="xh_cloudwork_exam.studentExam") {

               if (data.getString("Name")=="workFlag"){
                 val workFlag=data.getString("Value")
                 obj.put("workFlag",workFlag)
               }

               if (data.getString("Name")=="userId"){
                 val userId=data.getString("Value")
                 obj.put("userId",userId)
               }

               if (data.getString("Name")=="updateTime"){
                 val updateTime=data.getLong("Value")
                 val upTime=Utils.str2hour(updateTime)
                 obj.put("updateTime",updateTime)
                 obj.put("upTime",upTime)
               }
               if (data.getString("Name")=="answers"){
                 val answerarray=data.getJSONArray("Value")
                 for( b <- 0 to answerarray.size()-1) {
                   val array1: JSONArray = answerarray.get(b).asInstanceOf[JSONArray]
                   num= array1.get(2).asInstanceOf[JSONObject].getString("Value").toInt

                   if(obj.containsKey(num.toString))
                     obj.put(num.toString,obj.getInteger(num.toString).toInt+1)
                   else
                     obj.put(num.toString,1)
                 }

               }
             }
//             if (table == "xh_classroom_platform.ClassroomBase") {
//
//               if (data.getString("Name")=="workFlag"){
//                 val workFlag=data.getString("Value")
//                 obj.put("workFlag",workFlag)
//               }
//
//               if (data.getString("Name")=="updateTime"){
//                 val updateTime=data.getLong("Value")
//                 val upTime=Utils.str2hour(updateTime)
//                 obj.put("updateTime",updateTime)
//                 obj.put("upTime",upTime)
//               }
//               if (data.getString("Name")=="answers"){
//                 val answerarray=data.getJSONArray("Value")
//                 for( b <- 0 to answerarray.size()-1) {
//                   val array1: JSONArray = answerarray.get(b).asInstanceOf[JSONArray]
//                   num= array1.get(2).asInstanceOf[JSONObject].getString("Value").toInt
//
//                   //                   if(emptyMap.contains(num))
//                   //                     emptyMap += (num -> (emptyMap(num)+1))
//                   //                   else
//                   //                     emptyMap+= (num -> 1)
//
//                   if(obj.containsKey(num.toString))
//                     obj.put(num.toString,obj.getInteger(num.toString).toInt+1)
//                   else
//                     obj.put(num.toString,1)
//                 }
//
//               }
//             }
             if (table == "xh_king.game") {

               if (data.getString("Name")=="isHomeWork"){
                 isHomeWork=data.getInteger("Value")
                 obj.put("isHomeWork",isHomeWork)
               }
               if (data.getString("Name") == "timeConsuming") {
                 val timeConsuming = data.getString("Value")
                 obj.put("timeConsuming", timeConsuming)
               }
               if (data.getString("Name") == "studentId") {
                 val studentId = data.getString("Value")
                 obj.put("studentId", studentId)
               }
              if(isHomeWork==1) {

                if (data.getString("Name") == "subjectId") {
                  val subjectId = data.getString("Value")
                  obj.put("subjectId", subjectId)
                }

                if (data.getString("Name") == "questionCount") {
                  val questionCount = data.getString("Value")
                  obj.put("count", questionCount)
                }

                if (data.getString("Name") == "updateTime") {
                  val updateTime = data.getLong("Value")
                  val upTime = Utils.str2hour(updateTime)
                  obj.put("updateTime", updateTime)
                  obj.put("upTime", upTime)
                }
//                if (data.getString("Name") == "questionList") {
//                  val answerarray = data.getJSONArray("Value")
//                  for (b <- 0 to answerarray.size() - 1) {
//                    val array1: JSONArray = answerarray.get(b).asInstanceOf[JSONArray]
//                    for (a <- 0 to array1.size() - 1) {
//                      val data1 = array1.get(a).asInstanceOf[JSONObject]
//                      if (data1.getString("Name") == "valid") {
//                        valid = data1.getString("Value")
//                      }
//                      if (data1.getString("Name") == "parentQuestionId") {
//                        parentQuestionId = data1.getString("Value")
//                      }
//                    }
//                    if (valid == "true" && parentQuestionId == "0") {
//                      count += 1
//                    }
//                    valid = "true"
//                    parentQuestionId = "0"
//                  }
//                  obj.put("count", count)
//                }
              }
             }
//             if(table.contains("AnswerSet")&&sql_type=="u"){
//
//               if (data.getString("Name")=="$set"){
//
//                 val datax=data.getJSONArray("Value")
//                 for( a <- 0 to datax.size()-1) {
//                   val datas = datax.get(a).asInstanceOf[JSONObject]
//                   if(datas.getString("Name")=="answers"){
//                     countan=datas.getJSONArray("Value").size()
//                   }
//                   sum+=countan
//                 }
//               }
//
//                 val xx=System.currentTimeMillis()
//                 val upTime=Utils.str2hour(xx)
//                 obj.put("updateTime",xx)
//                 obj.put("upTime",upTime)
//
//               obj.put("sum",sum)
//             }
//             if(table=="xh_classroom_platform.Desktop" && sql_type=="u"){
//
//               if (data.getString("Name")=="$set"){
//
//                 val datax=data.getJSONArray("Value")
//                 for( a <- 0 to datax.size()-1) {
//                   val datas = datax.get(a).asInstanceOf[JSONObject]
//                   val name=datas.getString("Name")
//                   if(name.contains("desktopList")&& name.contains("taskList")){
//                     countan1=datas.getJSONArray("Value").size()
//                   }
//                   sum1+=countan1
//                   countan1=0
//                 }
//               }
//               val xx=System.currentTimeMillis()
//               val upTime=Utils.str2hour(xx)
//               obj.put("updateTime",xx)
//               obj.put("upTime",upTime)
//               obj.put("sum1",sum1)
//             }
             if (table.contains("ClassroomBase") && sql_type=="i") {

               if(table.contains("xh_classroom_platform")) obj.put("app","云课堂")
               if(table.contains("xh_classroom_platform_air")) obj.put("app","联云课")
               if (data.getString("Name")=="classroomId"){
                 val classroomId=data.getString("Value")
                 obj.put("classroomId",classroomId)
               }
               if (data.getString("Name")=="classroomName"){
                 val classroomName=data.getString("Value")
                 obj.put("classroomName",classroomName)
               }

               if (data.getString("Name")=="_id"){
                 val id=data.getString("Value")
                 obj.put("id",id)
               }

               if (data.getString("Name")=="userId"){
                 val UserId=data.getString("Value")
                 obj.put("UserId",UserId)
               }
               if (data.getString("Name")=="subject"){
                 val subject=Utils.null20(data.getString("Value"))
                 obj.put("subject",subject)
               }
               if (data.getString("Name")=="totalIds"){
                 val pre_count=data.getJSONArray("Value").size()
                 obj.put("pre_count",pre_count)
               }

                 val updateTime=System.currentTimeMillis()
                 val upTime=Utils.str2hour(updateTime)
                 obj.put("updateTime",updateTime)
                 obj.put("upTime",upTime)
             }

             if(table.contains("ClassroomBase")&&sql_type=="u"){

               if (data.getString("Name")=="$set"){

                 val datax=data.getJSONArray("Value")
                 for( a <- 0 to datax.size()-1) {
                   val datas = datax.get(a).asInstanceOf[JSONObject]
                   if(datas.getString("Name")=="totalIds"){
                     countan=datas.getJSONArray("Value").size()
                     obj.put("count",countan)
                   }
                 }
               }

               val xx=System.currentTimeMillis()
               val upTime=Utils.str2hour(xx)
               obj.put("updateTime",xx)
               obj.put("upTime",upTime)


             }
//             if(table.contains("ClassroomTemperature") && sql_type=="u"){
//               if (data.getString("Name")=="$set"){
//                 val datax=data.getJSONArray("Value")
//                 for( a <- 0 to datax.size()-1) {
//                   val datas = datax.get(a).asInstanceOf[JSONObject]
//                   val name=datas.getString("Name")
//                   if(name=="classroomTemperatureDetails"){
//                     countan1=datas.getJSONArray("Value").size()
//                   }
//                   sum1+=countan1
//                   countan1=0
//                 }
//               }
//               val xx=System.currentTimeMillis()
//               val upTime=Utils.str2hour(xx)
//               obj.put("updateTime",xx)
//               obj.put("upTime",upTime)
//               obj.put("sum1",sum1)
//             }
           }
           obj
         }
         catch {
           case e: Exception => {
             //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
             log.error("峰哥实时数据异常：%s, \\r\\n %s".format(e, x))
             println("原数据问题"+e)
             JSON.parseObject("{}")
           }
         }
       })
       .filter(_.getLong("updateTime")!=null)
       .filter(_.size()>4)

       .assignAscendingTimestamps(_.getLong("updateTime")*2000)
       .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(5))

      .apply(new ByWindow())
      .addSink(new  MySqlSink2())

     //.print()

    env.execute("all_yunzuoye_tizhou_2")
  }
}

class ByWindow() extends AllWindowFunction[JSONObject, Iterable[JSONObject], TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[JSONObject], out: Collector[Iterable[JSONObject]]): Unit = {



    if(input.nonEmpty) {
      //System.out.println("1 秒内收集到 接口的条数是：" + input.size)

      out.collect(input)
        }
  }
}


class MySqlSink2() extends RichSinkFunction[Iterable[JSONObject]] with Constants {
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var insertAutoCorrect: PreparedStatement = _
  var insertTiZhou: PreparedStatement = _
  var insertYunkeTang: PreparedStatement = _
  var inserWorkID: PreparedStatement = _
  var inserHandInNum: PreparedStatement = _
  var updatePreCount: PreparedStatement = _
  var inserHandInCount : PreparedStatement = _

  var result: ResultSet = null
  var updateStmt: PreparedStatement = _
  var table= ""
  var sql_type=""
  var workFlag124=0
   val Map = new mutable.HashMap[String, String]()
  val teacherInfoMap = new mutable.HashMap[Int, Int]()
  val workStatusMap = new mutable.HashMap[String, String]()
    var workFlag=""
  import org.apache.commons.dbcp2.BasicDataSource

  var dataSource: BasicDataSource = null
  var subject_id=0
  var teacher_id=0
 var check_num=0
  var emptyMap = new mutable.HashMap[Int, String]()
  var workMap = new mutable.HashMap[Int, String]()

  var  user_id =0
  var school_id=0
  var user_name=""
  var user_type=0
  var school_name=""
  var  city=""
  var  province=""
  var  city_name=""
  var countyname=""
 var  workinfo=""
  var proof=99



  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

      //Class.forName("com.mysql.jdbc.Driver")
      //conn = DriverManager.getConnection(Url, User, Password)
      import org.apache.commons.dbcp2.BasicDataSource

      dataSource = new BasicDataSource
      conn = getConnection(dataSource)
      //conn.setAutoCommit(false) // 开始事务

      insertStmt = conn.prepareStatement("iNSERT INTO all_real_yunzuoye (\n\tworks,\n\tworks_num,\n\tsubject_id,\n\tuptime,\n  cur_hour\n,cur_day,subject_name,school_id,school_name,province,city,county)\nVALUES\n\t(?, ?, ?, ?, ?,?,?, ?, ?,?,?,?) ON DUPLICATE KEY UPDATE works = works + ?,\n\tworks_num = works_num + ?,\n\tuptime=?")
      //insertAutoCorrect=conn.prepareStatement("INSERT INTO all_auto_correct_count (\n\tauto_num,\n  uptime,\n  cur_day,\ncur_hour\n)\nVALUES\n\t(?, ?, ?, ?) ON DUPLICATE KEY UPDATE auto_num = auto_num + ?,uptime=?")
      insertTiZhou=conn.prepareStatement("INSERT INTO all_real_tizhou(zilian_num,zilian_topic_count,zilian_consum,subject_id,subject_name,uptime,cur_hour,cur_day,school_id,school_name,province,city,county) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE zilian_num=zilian_num+1,zilian_topic_count=zilian_topic_count+?,zilian_consum=zilian_consum+?,uptime=?")
      insertYunkeTang=conn.prepareStatement("INSERT INTO all_yunketang_real (room_id, room_name, pre_count, online_count, max_count, status, subject_id, subject_name, statusUptime1to2,statusUphour1to2, cur_day, create_time, app,statusUptime2to3,statusUphour2to3,school_id,school_name,province,city,id,county) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE school_id=?")
      inserWorkID=conn.prepareStatement("INSERT INTO all_yunzuoye_work (\n\twork_id,\n\tproof,\n\tuptime\n ,handinnum ,school_id,topic_num,upto_time,province,city,county,school_name)\nVALUES\n\t(?, ?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE uptime=?")
      inserHandInNum=conn.prepareStatement("    iNSERT INTO all_yunzuoye_handin (\n work_id, handin_num,\n        check_num,\n        uptime,\n        cur_hour\n        ,cur_day)\n      VALUES\n      (?, ?, ?, ?, ?,?) ON DUPLICATE KEY UPDATE handin_num = ?,\n      check_num = check_num + ?,\n      uptime=?")
      updatePreCount=conn.prepareStatement(" UPDATE all_yunketang_real set pre_count=? where id =?")
      inserHandInCount=conn.prepareStatement("iNSERT INTO all_hand_num (\n        handin_num,\n       \n        uptime,\n        cur_hour\n        ,cur_day,school_id,province,city,county,school_name)\n      VALUES\n      (?, ?, ?, ?,?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE handin_num = handin_num+1,\n      uptime=?")

    //教师对应科目信息将数据放在内存中
    try {
//      val qusubjectInfo = "select teacher_id,subject_id from fact_teacher_info"
//      val results1: ResultSet = MysqlUtils1.select(qusubjectInfo)
//      while (results1.next()) {
//        val teacher_id = results1.getInt(1)
//        val subject_id = results1.getInt(2)
//        teacherInfoMap+= (teacher_id ->subject_id)
//      }

      val proofStatusInfo = "select work_id,proof,school_id,province,city,county,school_name from all_yunzuoye_work"
      val results2: ResultSet = MysqlUtils2.select(proofStatusInfo)
      while (results2.next()) {
        val work_id = results2.getString(1)
        val proof= results2.getInt(2)
        school_id = results2.getInt(3)
        province= results2.getString(4)
        city = results2.getString(5)
        countyname = results2.getString(6)
        school_name = results2.getString(7)
        workinfo= proof+ "#"  +school_id + "#"  + province+"#" + city+"#" + countyname+"#" + school_name
        workStatusMap+= (work_id ->workinfo)
      }
    }
    catch {
      case e: Exception => {
        println("open 函数将数据放到内存中失败"+e)
      }
    }


    try {
      val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \nxh_user_service.XHSys_User a\nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"

      val results: ResultSet = MysqlUtils.select(quUserInfoSql)
      while (results.next()) {

        user_id =results.getInt(1)
        school_id=results.getInt(2)
        user_name=results.getString(3)
        user_type=results.getInt(4)
        school_name=results.getString(5)
        city=results.getString(6)
        province=results.getString(7)
        city_name=results.getString(8)
        val info=user_id + "#" + school_id + "#" + school_name + "#" + province+"#" + city_name+"#" + city
        emptyMap += (user_id -> info)
      }






    }
    catch {
      case e: Exception => {
        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"open函数用户信息出现问题"+e)
      }
    }
  }



  // 调用连接，执行sql
  override def invoke(values: Iterable[JSONObject], context: SinkFunction.Context[_]): Unit = {

      for(value <-values) {
        try {
          table = value.getString("table")
          sql_type = value.getString("sql_type")
          if ((table == "xh_cloudwork_parts.work" || table == "xh_cloudwork_exam.teacherExam") && sql_type == "i") {
            insertStmt.setInt(1, value.getInteger("handInNum"))
            insertStmt.setInt(2, value.getInteger("handInNum") * value.getInteger("topicNum"))


            val subject_id = value.getInteger("subject")
            val subject_name = subjectMap.getOrElse(subject_id, "")
            val userid = value.getInteger("teacherId")

            val userinfo = emptyMap.getOrElse(userid, "信息错误")
            if (userinfo != "信息错误") {
              user_id = userinfo.split("#")(0).toInt
              school_id = userinfo.split("#")(1).toInt
              school_name = userinfo.split("#")(2)
              province = userinfo.split("#")(3)
              city_name = userinfo.split("#")(4)
              countyname = userinfo.split("#")(5)
            }
            if (userinfo == "信息错误") {

              val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \n(select * from xh_user_service.XHSys_User where  iUserId=" + user_id + ") a \nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"
              val results: ResultSet = MysqlUtils.select(quUserInfoSql)
              while (results.next()) {
                user_id = results.getInt(1)
                school_id = results.getInt(2)
                //user_name=results.getString(3)
                //user_type=results.getInt(4)
                school_name = results.getString(5)
                countyname=results.getString(6)
                province = results.getString(7)
                city_name = results.getString(8)
              }
            }
            insertStmt.setInt(3, value.getInteger("subject"))
            insertStmt.setString(4, value.getString("upTime"))
            insertStmt.setInt(5, value.getString("upTime").split(" ")(1).split(":")(0).toInt)
            insertStmt.setString(6, value.getString("upTime").split(" ")(0))
            insertStmt.setString(7, subject_name)
            insertStmt.setInt(8, school_id)
            insertStmt.setString(9, school_name)
            insertStmt.setString(10, province)
            insertStmt.setString(11, city_name)
            insertStmt.setString(12, countyname)
            insertStmt.setInt(13, value.getInteger("handInNum"))
            insertStmt.setInt(14, value.getInteger("handInNum") * value.getInteger("topicNum"))
            insertStmt.setString(15, value.getString("upTime"))

            inserWorkID.setString(1, value.getString("id"))
            inserWorkID.setInt(2, value.getInteger("proofreadState"))
            inserWorkID.setString(3, value.getString("upTime"))
            inserWorkID.setInt(4, value.getInteger("handInNum"))
            inserWorkID.setInt(5, school_id)
            inserWorkID.setInt(6, value.getInteger("topicNum"))
            inserWorkID.setString(7, value.getString("upto_time"))
            inserWorkID.setString(8, province)
            inserWorkID.setString(9, city_name)
            inserWorkID.setString(10, countyname)
            inserWorkID.setString(11, school_name)
            inserWorkID.setString(12, value.getString("upTime"))
            inserWorkID.addBatch()
            insertStmt.addBatch()
          }
          if ((table == "xh_cloudwork_parts.work" || table == "xh_cloudwork_exam.teacherExam") && sql_type == "u") {

                      val handin = Utils.null20(value.getString("handin"))
                      if (handin != 0) {
                        val work_id = value.getString("id")
                        val workinfo = workStatusMap.get(work_id)

                        if(workinfo==Some) {
                           proof = workStatusMap.get(work_id).get.split("#")(0).toInt
                          school_id = workStatusMap.get(work_id).get.split("#")(1).toInt
                          province = workStatusMap.get(work_id).get.split("#")(2)
                          city = workStatusMap.get(work_id).get.split("#")(3)
                          countyname = workStatusMap.get(work_id).get.split("#")(4)
                          school_name = workStatusMap.get(work_id).get.split("#")(5)
                        }
                       else
                         {
                          val quproofInfo = "select proof,school_id,province,city,county ,school_name from all_yunzuoye_work where work_id='" + work_id + "'"
                          val results1: ResultSet = MysqlUtils2.select(quproofInfo)
                          while (results1.next()) {
                            proof = results1.getInt(1)
                            school_id = results1.getInt(2)
                            province= results1.getString(3)
                            city = results1.getString(4)
                            countyname = results1.getString(5)
                            school_name=results1.getString(6)
                          }
                        }
                        if (proof == 0) check_num = 1
                        inserHandInNum.setString(1, work_id)
                        inserHandInNum.setInt(2, handin)
                        inserHandInNum.setInt(3, check_num)
                        inserHandInNum.setString(4, value.getString("upTime"))
                        inserHandInNum.setInt(5, value.getString("upTime").split(" ")(1).split(":")(0).toInt)
                        inserHandInNum.setString(6, value.getString("upTime").split(" ")(0))
                        inserHandInNum.setInt(7, handin)
                        inserHandInNum.setInt(8, check_num)
                        inserHandInNum.setString(9, value.getString("upTime"))
                        check_num = 0

                        inserHandInCount.setInt(1, 1)
                        inserHandInCount.setString(2, value.getString("upTime"))
                        inserHandInCount.setInt(3, value.getString("upTime").split(" ")(1).split(":")(0).toInt)
                        inserHandInCount.setString(4, value.getString("upTime").split(" ")(0))
                        inserHandInCount.setInt(5, school_id)
                        inserHandInCount.setString(6, province)
                        inserHandInCount.setString(7, city)
                        inserHandInCount.setString(8, countyname)
                        inserHandInCount.setString(9, school_name)
                        inserHandInCount.setString(10, value.getString("upTime"))
                        inserHandInCount.addBatch()
                        inserHandInNum.addBatch()
                      }
                    }
          if ((table == "xh_cloudwork_parts.studentWork" || table == "xh_cloudwork_exam.studentExam") && sql_type == "uu") {
            workFlag = Utils.null2kong(value.getString("workFlag"))

            if (workFlag == "1" || workFlag == "2") {
              workFlag124 = Utils.null20(value.getInteger("1")) + Utils.null20(value.getInteger("2")) + Utils.null20(value.getInteger("4")) + Utils.null20(value.getInteger("10"))
            }
            if (workFlag == "4") {
              workFlag124 = Utils.null20(value.getInteger("1")) + Utils.null20(value.getInteger("2")) + Utils.null20(value.getInteger("3")) + Utils.null20(value.getInteger("6"))
            }
            if (workFlag124 != 0) {
              Map += (value.getString("upTime") -> "4")
              insertAutoCorrect.setInt(1, workFlag124)
              insertAutoCorrect.setString(2, value.getString("upTime"))
              insertAutoCorrect.setString(3, value.getString("upTime").split(" ")(0))
              insertAutoCorrect.setInt(4, value.getString("upTime").split(" ")(1).split(":")(0).toInt)
              insertAutoCorrect.setInt(5, workFlag124)
              insertAutoCorrect.setString(6, value.getString("upTime"))
              insertAutoCorrect.addBatch()
            }
          }
          if (table == "xh_king.game" && Utils.null20(value.getInteger("isHomeWork")) == 1) {
            val timeConsume = Utils.null20(value.getInteger("timeConsuming"))
            val count = Utils.null20(value.getInteger("count"))
            val subject_id = Utils.null20(value.getInteger("subjectId"))
            val subject_name = subjectMap.getOrElse(subject_id, "")
            val userid = value.getInteger("studentId")
            val userinfo = emptyMap.getOrElse(userid, "信息错误")
            if (userinfo != "信息错误") {
              user_id = userinfo.split("#")(0).toInt
              school_id = userinfo.split("#")(1).toInt
              school_name = userinfo.split("#")(2)
              province = userinfo.split("#")(3)
              city_name = userinfo.split("#")(4)
              countyname= userinfo.split("#")(5)
            }
            if (userinfo == "信息错误") {
              val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \n(select * from xh_user_service.XHSys_User where  iUserId=" + user_id + ") a \nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"

              val results: ResultSet = MysqlUtils.select(quUserInfoSql)
              while (results.next()) {
                user_id = results.getInt(1)
                school_id = results.getInt(2)
                //user_name=results.getString(3)
                //user_type=results.getInt(4)
                school_name = results.getString(5)
                countyname=results.getString(6)
                province = results.getString(7)
                city_name = results.getString(8)
              }

            }
            insertTiZhou.setInt(1, 1)
            insertTiZhou.setInt(2, count)
            insertTiZhou.setInt(3, timeConsume)
            insertTiZhou.setInt(4, subject_id)
            insertTiZhou.setString(5, subject_name)
            insertTiZhou.setString(6, value.getString("upTime"))
            insertTiZhou.setInt(7, value.getString("upTime").split(" ")(1).split(":")(0).toInt)
            insertTiZhou.setString(8, value.getString("upTime").split(" ")(0))
            insertTiZhou.setInt(9, school_id)
            insertTiZhou.setString(10, school_name)
            insertTiZhou.setString(11, province)
            insertTiZhou.setString(12, city_name)
            insertTiZhou.setString(13, countyname)
            insertTiZhou.setInt(14, count)
            insertTiZhou.setInt(15, timeConsume)
            insertTiZhou.setString(16, value.getString("upTime"))
            //维度信息

            insertTiZhou.addBatch()
          }
          if (table.contains("ClassroomBase") && sql_type == "i") {
            val userid = value.getInteger("UserId")
            subject_id = Utils.null20(value.getInteger("subject"))
            if (subject_id == 0) {
              val qusubjectInfo = "select subject_id from fact_teacher_info where teacher_id=" + userid + " order by subject_id limit 1"
              val results1: ResultSet = MysqlUtils1.select(qusubjectInfo)
              while (results1.next()) {
                subject_id = results1.getInt(1)
              }
            }
            val userinfo = emptyMap.getOrElse(userid, "信息错误")
            if (userinfo != "信息错误") {
              user_id = userinfo.split("#")(0).toInt
              school_id = userinfo.split("#")(1).toInt
              school_name = userinfo.split("#")(2)
              province = userinfo.split("#")(3)
              city_name = userinfo.split("#")(4)
              countyname = userinfo.split("#")(5)

            }
            if (userinfo == "信息错误") {

              val quUserInfoSql = "select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname,b.sProvinceName,b.sCityName  from \n(select * from xh_user_service.XHSys_User where  iUserId=" + user_id + ") a \nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"
              val results: ResultSet = MysqlUtils.select(quUserInfoSql)
              while (results.next()) {

                user_id = results.getInt(1)
                school_id = results.getInt(2)
                //user_name=results.getString(3)
                //user_type=results.getInt(4)
                school_name = results.getString(5)
                countyname=results.getString(6)
                province = results.getString(7)
                city_name = results.getString(8)
              }

            }
            val subject_name = subjectMap.getOrElse(subject_id, "")
            insertYunkeTang.setString(1, value.getString("classroomId"))
            insertYunkeTang.setString(2, value.getString("classroomName"))
            insertYunkeTang.setInt(3, value.getInteger("pre_count"))
            insertYunkeTang.setInt(4, 0)
            insertYunkeTang.setInt(5, 0)
            insertYunkeTang.setInt(6, 1)
            insertYunkeTang.setInt(7, subject_id)
            insertYunkeTang.setString(8, subject_name)
            insertYunkeTang.setString(9, "0")
            insertYunkeTang.setInt(10, 0)
            insertYunkeTang.setString(11, value.getString("upTime").split(" ")(0))
            insertYunkeTang.setString(12, value.getString("upTime"))
            insertYunkeTang.setString(13, value.getString("app"))
            insertYunkeTang.setString(14, "0")
            insertYunkeTang.setInt(15, 0)
            //用户信息
            insertYunkeTang.setInt(16, school_id)


            insertYunkeTang.setString(17, school_name)
            insertYunkeTang.setString(18, province)
            insertYunkeTang.setString(19, city_name)
            insertYunkeTang.setString(20, value.getString("id"))
            insertYunkeTang.setString(21, countyname)
            insertYunkeTang.setInt(22, school_id)

            insertYunkeTang.addBatch()

            user_id = 0
            school_id = 0
            user_name = ""
            user_type = 0
            school_name = ""
            city = ""
            province = ""
            city_name = ""
          }


          if (table.contains("ClassroomBase") && sql_type == "u") {
            val id = value.getString("id")

            val count = Utils.null20(value.getInteger("count"))
            if (count != 0) {
              updatePreCount.setInt(1, count)
              updatePreCount.setString(2, id)
              updatePreCount.addBatch()
            }
          }
        }

        catch {
          case e: Exception => {
            log.error("数据异常：%s, \\r\\n %s".format(e, values))
            print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"本条数据异常" + e + value)
          }
        }
      }

    try {
    // val count1 = insertAutoCorrect.executeBatch //批量后执行
      val count2 = insertStmt.executeBatch //批量后执行
      val count3 = insertTiZhou.executeBatch //批量后执行
      val count4 = insertYunkeTang.executeBatch //批量后执行
      val count5 = inserWorkID.executeBatch //批量后执行
      val count6 = inserHandInNum.executeBatch //批量后执行
      val count7 = updatePreCount.executeBatch //批量后执行
      val count8 =inserHandInCount.executeBatch()
//      System.out.println("云作业成功了插入了了" + count2.length + "行数据")
//      System.out.println("题舟成功了插入了了" + count3.length + "行数据")
//      System.out.println("云课堂成功了插入了了" + count4.length + "行数据")
//      System.out.println("work表了插入了了" + count5.length + "行数据")
//      System.out.println("已经截至交的作业表插入了" + count6.length + "行数据")
//      System.out.println("预设人数成功了插入了了" + count7.length + "行数据")
//      System.out.println("上交人数成功了插入了了" + count8.length + "行数据")

      //conn.commit
    }

    catch {
      case e: Exception => {
        log.error("数据异常：%s, \\r\\n %s".format(e, values))
        print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"提交数据发生异常" + e )
      }
    }
//
     // System.out.println("接口访问量成功了插入了了" + count1.length + "行数据")



  }

  // 关闭时做清理工作
  override def close(): Unit = {
    try {
      //insertAutoCorrect.close()
      insertStmt.close()
      insertTiZhou.close()
      insertYunkeTang.close()
      inserWorkID.close()
      inserHandInNum.close()
      updatePreCount.close()
      conn.close()
    //   println("云mysql关闭成功")
    } catch {
      case e: Exception => {
        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+"云mysql关闭失败"+e)
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