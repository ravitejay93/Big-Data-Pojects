def get_avg(rating:Iterable[String]): Double = {
  var count:Double = 0.0
  var num:Double = 0.0;
  var it = rating.iterator
  while(it.hasNext){
    count = count + it.next().toDouble
    num = num +1.0;
  }
  count/num
}
def get_string(s:Array[String]): String = {
  var result:String = ""
  s.foreach(result += _ + "\t")
  result
}
val b_file = sc.textFile("/yelp/business/business.csv")
val r_file = sc.textFile("/yelp/review/review.csv")
val b_rows = b_file.map(_.split('^')).filter(x => x.length == 3).keyBy(x=> x(0))
val r_rows = r_file.map(_.split('^')).map(l => (l(2),l(3)))
val avg_rating = r_rows.groupByKey().map(x => (x._1,get_avg(x._2)))
val join_data = b_rows.join(avg_rating).collect().sortBy(x=> x._2._2).reverse.take(10).foreach(x => println(get_string(x._2._1)+ x._2._2))
