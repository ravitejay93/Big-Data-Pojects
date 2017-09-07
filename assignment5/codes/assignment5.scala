// Databricks notebook source
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val rawData = sc.textFile("/FileStore/tables/lwqooo3a1500614925606/CA_HepTh-9cc05.txt")

val rawRDD = rawData.map(_.split("\t")).filter(x=> !x(0).contains('#'))


// COMMAND ----------

val vertices: RDD[(VertexId,(String))] = rawRDD.flatMap(x=> List(x(0),x(1))).distinct.map(x=> (x.toLong,("NA"))).sortByKey()

vertices.count()

// COMMAND ----------

val edges : RDD[Edge[Int]] = rawRDD.map(x=> Edge(x(0).toLong,x(1).toLong,1))

edges.collect()

// COMMAND ----------

val graph = Graph(vertices,edges,("NA"))

// COMMAND ----------

val outDegree = graph.outDegrees.sortBy(x => -1*x._2)

val sum_out = outDegree.map(_._2).sum()
val rdd = outDegree.take(1)

println("vertex: " + rdd(0)._1 + " " + rdd(0)._2 + ", Total outDegree: " + sum_out)

// COMMAND ----------

val inDegree = graph.inDegrees.sortBy(x => -1*x._2)

val rdd = inDegree.take(1)

val sum_in = inDegree.map(_._2).sum()

println("vertex: " + rdd(0)._1 + " " + rdd(0)._2 + ", Total inDegree: " + sum_in)

// COMMAND ----------

val ranks = graph.pageRank(0.0001).vertices.sortBy(x => -1*x._2)

ranks.take(5).foreach(println)

// COMMAND ----------

val cc = graph.connectedComponents().vertices
cc.collect().foreach(println)

// COMMAND ----------

val triangleCounts = graph.triangleCount().vertices.sortBy(x=> -1*x._2)
triangleCounts.take(5).foreach(println)

// COMMAND ----------


