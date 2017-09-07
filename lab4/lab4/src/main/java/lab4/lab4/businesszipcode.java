package lab4.lab4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import lab4.lab4.MiscUtils;

public class businesszipcode {
public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length ==3) {
				String[] subset = StringUtils.split(businessData[1]," ");
				
				if(subset[subset.length-1].length() == 5)context.write(new Text(subset[subset.length-1]), new IntWritable(1));
				
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
		//private List<Pair<Text,IntWritable>> l = new ArrayList<Pair<Text, IntWritable>>();
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
		
			int count = 0;
			for(IntWritable t : values){
				//context.write(key,t);
				count += t.get();
			}
			countMap.put(new Text(key),new IntWritable(count));
			
		}
		@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {

	        Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);
	        
	        int counter = 0;
	        for (Text key : sortedMap.keySet()) {
	            if (counter++ == 20) {
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
		job.setJarByClass(businesszipcode.class);
	   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
				
		job.setOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/yelp/business/business.csv"));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

