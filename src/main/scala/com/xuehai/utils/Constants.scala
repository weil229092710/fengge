package com.xuehai.utils

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by root on 2019/11/13.
  */
trait Constants {

	val LOG = LoggerFactory.getLogger(this.getClass)

	/**
	  * kafka
	  */
	val brokerList = PropertiesUtil.getKey("brokerList")
	val topic = PropertiesUtil.getKey("topicName")
	val xaingyingtopic = PropertiesUtil.getKey("topicXiangyingName")
	val onlinetopic = PropertiesUtil.getKey("topicOnlineName")
	val mongotopic = PropertiesUtil.getKey("topicMongoName")
	val chanaltopic = PropertiesUtil.getKey("topicChanal")


	val groupId = PropertiesUtil.getKey("kafkaGroupId")
	val props = new Properties()
	props.put("bootstrap.servers", brokerList)
	props.put("auto.offset.reset", PropertiesUtil.getKey("offset"))//earliest  latest
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
	 * mysql
	 */
	val mysqlHost1 = PropertiesUtil.getKey("mysql_host1")
	val mysqlPort1 = PropertiesUtil.getKey("mysql_port1")
	val mysqlUser1 = PropertiesUtil.getKey("mysql_user1")
	val mysqlPassword1 = PropertiesUtil.getKey("mysql_password1")
	val mysqlDB1 = PropertiesUtil.getKey("mysql_db1")
	val mysqlUtilsUrl1 = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(mysqlHost1, mysqlPort1, mysqlDB1)



	/**
	 * mysql
	 */
	val mysqlHost2 = PropertiesUtil.getKey("mysql_host2")
	val mysqlPort2 = PropertiesUtil.getKey("mysql_port2")
	val mysqlUser2 = PropertiesUtil.getKey("mysql_user2")
	val mysqlPassword2 = PropertiesUtil.getKey("mysql_password2")
	val mysqlDB2 = PropertiesUtil.getKey("mysql_db2")
	val mysqlUtilsUrl2 = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(mysqlHost2, mysqlPort2, mysqlDB2)



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



  val subjectMap = new mutable.HashMap[Int, String]()
  subjectMap+=(1->"语文")
  subjectMap+=(2->"数学")
  subjectMap+=(3->"英语")
  subjectMap+=(4->"科学")
  subjectMap+=(5->"历史与社会")
  subjectMap+=(6->"道德与法治")
  subjectMap+=(7->"物理")
  subjectMap+=(8->"化学")
  subjectMap+=(9->"生物")
  subjectMap+=(10->"地理")
  subjectMap+=(11->"文综")
  subjectMap+=(12->"理综")
  subjectMap+=(13->"通用技术")
  subjectMap+=(14->"政治")
  subjectMap+=(15->"信息技术")
  subjectMap+=(16->"历史")
  subjectMap+=(17->"美术")
  subjectMap+=(18->"音乐")
  subjectMap+=(19->"综合")
}
