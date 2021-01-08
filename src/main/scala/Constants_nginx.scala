import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by root on 2019/11/13.
  */
trait Constants_nginx {

	val LOG = LoggerFactory.getLogger(this.getClass)

	/**
	  * kafka
	  */
	val brokerList = PropertiesUtil.getKey("brokerList")
	val topic = PropertiesUtil.getKey("topicName")
	val onlinetopic = PropertiesUtil.getKey("topicOnlineName")

	val groupId = PropertiesUtil.getKey("kafkaGroupId")
	val props = new Properties()
	props.put("bootstrap.servers", brokerList)
	props.put("auto.offset.reset", "latest")//earliest   latest
	props.put("group.id", groupId)
	props.put("enable.auto.commit", "false")
	props.put("auto.commit.interval.ms", "1000")
	props.put("session.timeout.ms", "30000")
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
	props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

	/**
	  * checkPoint
	  */
	val checkPointPath = PropertiesUtil.getKey("checkPointPath")


	/**
		* mysql
		*/
	val mysqlHost = PropertiesUtil.getKey("mysql_host")
	val mysqlPort = PropertiesUtil.getKey("mysql_port")
	val mysqlUser = PropertiesUtil.getKey("mysql_user")
	val mysqlPassword = PropertiesUtil.getKey("mysql_password")
	val mysqlDB = PropertiesUtil.getKey("mysql_db")
	val mysqlUtilsUrl = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(mysqlHost, mysqlPort, mysqlDB)



	/**
		* 云mysql
		*/
	val Host = PropertiesUtil.getKey("Host")
	val Port = PropertiesUtil.getKey("Port")
	val User = PropertiesUtil.getKey("User")
	val Password = PropertiesUtil.getKey("Password")
	val DB = PropertiesUtil.getKey("DB")
	val Url = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(Host, Port, DB)





	/**
	  * log日志系统
	  */
	val log: Logger = LoggerFactory.getLogger(this.getClass)

	/**
	  * 钉钉机器人
	  */
	val DingDingUrl: String = PropertiesUtil.getKey("dingding_url")

	/**
	  * 任务名称
	  */
	val jobName = PropertiesUtil.getKey("jobName")


	val appNameMap = new mutable.HashMap[String, String]()
	appNameMap+=("com.xh.alsstu" ->"智通云听说")
	appNameMap+=("com.xh.alstch"-> "智通云听说")
	appNameMap+=("com.xh.aklestu"  -> "智通课堂3.x")
	appNameMap+=("com.xh.akletch"  -> "智通课堂3.x")
	appNameMap+=("com.xuehai.launcher" -> "智通平台")
	appNameMap+=("com.xuehai.response_launcher_teacher" ->"智通平台")
	appNameMap+=("com.xh.acldtch"-> "云作业")
	appNameMap+=("com.xh.acldstu"-> "云作业")
	appNameMap+=("com.xh.smartclassstu" -> "云课堂")
	appNameMap+=("com.xh.smartclasstch"  -> "云课堂")
	appNameMap+=("com.xh.areadunc" -> "悦读")
	appNameMap+=("com.xh.abrustu" ->"学海题舟")
	appNameMap+=("com.xh.abrutch"  ->"学海题舟")
	appNameMap+=("com.xh.ime" -> "学海输入法")
	appNameMap+=("com.xh.arespunc" -> "响应")
	appNameMap+=("com.xh.acbktch" ->"语文基础知识")
	appNameMap+=("com.xh.aartunc"  ->"美文与写作")
	appNameMap+=("com.zhitongyun.oetStudent"-> "口语训练")
	appNameMap+=("com.zhitongyun.oetteacher" -> "口语训练")
	appNameMap+=( "com.xh.feedback" -> "反馈")
	appNameMap+=("com.fen.afmtch"  -> "云辅导")
	appNameMap+=("com.fen.afmstu"  -> "云辅导")
	appNameMap+=("com.xh.ascstu" -> "联云课")
	appNameMap+=("com.xh.asctch" -> "联云课")
	appNameMap+=("com.xh.assist"-> "网络检查工具")
	appNameMap+=("com.xuehai.cwpmaker" -> "录课宝")
	appNameMap+=("com.xuehai.cwpplayer"-> "播放器")




}
