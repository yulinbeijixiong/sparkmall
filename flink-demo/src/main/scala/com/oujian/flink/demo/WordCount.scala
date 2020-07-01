package com.oujian.flink.demo


import org.apache.flink.api.scala._



object WordCount {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val ds: DataSet[String] = environment.readTextFile("C:\\Users\\Ann\\IdeaProjects\\sparkmall\\flink-demo\\src\\main\\resources\\word.txt")

    val value: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)


    value.print()


  }
}
