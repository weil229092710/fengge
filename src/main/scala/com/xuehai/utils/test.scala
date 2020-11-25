package com.xuehai.utils

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.alibaba.fastjson.JSON
import com.xuehai.utils.MysqlUtils.{conn, getMysqlConnection}

import scala.collection.mutable

object test {
  def main(args: Array[String]): Unit = {
    val aa="2020-09-15 15:32:11"

//   val aab=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
//    val strings = aab.split(" ")(1)
//    println(strings)

 val aas=str2hour(1604994672002L)
    println(aas)

    val str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(str)
    if((1==1||2==2)&&3==3){
      println("ss")
    }
  }


  def str2hour(x: Long): String ={
    import java.text.SimpleDateFormat
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time_Date = sdf.format(x)
    time_Date
  }
}
