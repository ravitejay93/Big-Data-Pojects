val b_file = sc.textFile("/yelp/business/business.csv")
val r_file = sc.textFile("/yelp/review/review.csv")

val b_rows = b_file.map(_.split('^')).filter(x => x(1).contains("TX")).map(x=> (x(0),1))
val r_rows = r_file.map(_.split('^')).map(x => (x(2),1)).reduceByKey(_+_)
val join_data = b_rows.join(r_rows).collect().sortBy(_._1).foreach(x => println(x._1 +"\t" +x._2._2))
