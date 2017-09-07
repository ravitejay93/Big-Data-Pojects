package lab4.lab4;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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


public class businessinny {
public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length ==3) {
				String[] subset = StringUtils.split(businessData[1]," ");
				for(int i = 0; i < subset.length;i++){
					if(subset[i].equals("NY") && businessData[2].contains("Restaurants"))context.write(new Text(businessData[0]), new Text(businessData[1]));
				}
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		
			
			for(Text t : values){
				context.write(key,t);
			}
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
					  
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(businessinny.class);
	   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
				
		job.setOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/yelp/business/business.csv"));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
