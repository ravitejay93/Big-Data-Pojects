package lab4.lab4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class reviewbusiness {
public static class BusinessMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			String delims = "^";
			String[] reviewData = StringUtils.split(value.toString(),delims);
			
			if (reviewData.length ==4) {
								
				context.write(new Text(reviewData[2]), new DoubleWritable(Double.valueOf(reviewData[3])));
				
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		private Map<Text, DoubleWritable> countMap = new HashMap<Text, DoubleWritable>();
		//private List<Pair<Text,IntWritable>> l = new ArrayList<Pair<Text, IntWritable>>();
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
		
			int count = 0;
			double sum = 0.0;
			for(DoubleWritable t : values){
				//context.write(key,t);
				sum += t.get();
				count++;
			}
			countMap.put(new Text(key),new DoubleWritable(sum/count));
			
		}
		@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {

	        Map<Text, DoubleWritable> sortedMap = MiscUtils.sortByValues(countMap);
	        
	        int counter = 0;
	        for (Text key : sortedMap.keySet()) {
	            if (counter++ == 10) {
	                break;
	            }
	            context.write(key, sortedMap.get(key));
	        }
	    }
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(reviewbusiness.class);
	   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/yelp/review/review.csv"));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
