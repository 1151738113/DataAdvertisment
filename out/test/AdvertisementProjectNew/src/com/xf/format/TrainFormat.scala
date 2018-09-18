package com.xf.format

import java.io.{File, PrintWriter}

import com.xf.util.{DataFormat, SparkConfig}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by wei.wang on 2018/9/13 0013.
  * 训练集格式化
  */
object TrainFormat extends Serializable{

  val sparkc = new SparkConfig()

  def downloadFile(): Unit = {

    val spark = sparkc.getSpark()

    SparkEnv.get.conf.set("spark.debug.maxToStringFields","200")

    spark.sparkContext.setLogLevel("WARN")

    val path = "data/round1_iflyad_test_feature.txt"
    //创建df，去表头
    val df = readFile(path,"\t",spark)

    //数据处理make、model列
    val df1 = DataFormat.oneHotEncording("make","makeNew",df,spark)
    val df2 = DataFormat.oneHotEncording("model","modelNew",df1,spark)
    val df2_new = df2.drop("make","model")
    val df1_new = df2_new.withColumnRenamed("makeNew","make").withColumnRenamed("modelNew","model")
    //创建table
//    df1_new.createTempView("data")

//    val writer = new PrintWriter(new File("E://learningScala.txt"))
    df1_new.select("make").show()
    df1_new.select("make").rdd.map{row =>
      val a1 = row.toSeq.toArray.map(x=>x.toString).apply(0)
      println(a1)
    }

//    writer.close()

  }

  def main(args: Array[String]): Unit = {
    downloadFile()
  }


  /**
    *
    * @param path 文件路径
    * @param tab 分隔符
    * @return
    */
  def readFile(path:String,tab:String,spark:SparkSession):DataFrame={
    val data = spark.sparkContext.textFile(path)
    //获取第一行
    val firstLine = data.first()
    val firstLines = firstLine.split(tab)
    //去表头
    val rdd = data.filter(_ != firstLine).map{line =>
      val str = line.split("\t")
      for(i <- 0 until firstLines.length){
        str(i).trim
      }
      Row(str:_*)
    }
    //给每列创建类型
    val struct = StructType(firstLines.map {
      x =>StructField(x,StringType,true)})
    spark.createDataFrame(rdd,struct)
  }

}
