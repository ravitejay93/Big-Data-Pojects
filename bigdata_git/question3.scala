// Databricks notebook source
val b_file = sc.textFile("/FileStore/tables/7bq6kr2x1498853141322/business.csv")
val r_file = sc.textFile("/FileStore/tables/i0f0z79f1498853210311/review.csv")

val b_rows = b_file.map(_.split('^')).filter(x => x(1).contains("Stanford")).map(x=> (x(0),1))
val r_rows = r_file.map(_.split('^')).map(l => (l(2),(l(1),l(3))))
val join_data = b_rows.join(r_rows).collect().foreach(x => println(x._2._2._1+"\t" + x._2._2._2))

// COMMAND ----------


