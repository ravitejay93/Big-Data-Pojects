val u_file = sc.textFile("/yelp/user/user.csv")
val r_file = sc.textFile("/yelp/review/review.csv")

val u_rows = u_file.map(_.split('^')).map(x => (x(0),x(1)))
val r_rows = r_file.map(_.split('^')).map(x => (x(1),1)).reduceByKey(_+_)
val join_data = r_rows.join(u_rows).collect().sortBy(x=> -1*x._2._1).take(10).foreach(x => println(x._1 + "\t" +x._2._2))
