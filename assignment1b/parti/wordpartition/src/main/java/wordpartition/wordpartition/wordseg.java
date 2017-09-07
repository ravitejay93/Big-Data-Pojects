package wordpartition.wordpartition;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class wordseg {
	
	public static ArrayList<String> positive = new ArrayList<String>();
	public static ArrayList<String> negative = new ArrayList<String>();
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text in = new Text();
		
		public void setup(Mapper.Context context) throws IOException{
			generate_reference();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String val = itr.nextToken();
				/*word.set(val);
				context.write(word, one);*/
				if(get_value(val) == 0){
					in.set("positive");
					context.write(in, one);
					
				}
				else if(get_value(val) == 1){
					in.set("negative");
					context.write(in, one);
				}
			}
		}
		
		public static void generate_reference() throws IOException{
			String line = null;
			
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader b = new BufferedReader(new InputStreamReader(fs.open(new Path("/user/rxy160030/input/negative-words.txt"))));
			
			while((line=b.readLine())!= null){
				negative.add(line);
				//System.out.println(line);
			}
			
			b = new BufferedReader(new InputStreamReader(fs.open(new Path("/user/rxy160030/input/positive-words.txt"))));
			while((line=b.readLine())!= null){
				positive.add(line);
				//System.out.println(line);
			}
			
		}
		
		public static int get_value(String input){
			
			int n = binary_search(1,input);
			//int n = negative.indexOf(input);
			if(n != -1){
				return 1;
			}
			int p = binary_search(0,input);
			//int p = positive.indexOf(input);
			if(p != -1){
				return 0;
			}
			return -1;
		}
		
		public static int binary_search(int n,String val){
			ArrayList<String> buff = (n == 0? positive:negative);
			
			int low = 0;
			int high = buff.size()-1;
			
			while(low < high){
				int mid = low+(high-low)/2;
				if(buff.get(mid).equals(val)) return mid;
				if(buff.get(mid).compareTo(val) > 0) high = mid;
				else low = mid+1;
			}
			
			return -1;
			
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				
			}
			result.set(sum);
			context.write(new Text("Total count of"+ key.toString() + "words:"), result);
		}
	}
	
	public static void main(String[] args){
		
	    try {
	    	Configuration conf = new Configuration();
		    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		    conf.set("mapreduce.framework.name", "yarn");
		    Job job = Job.getInstance(conf, "word_segmentation");
		    job.setJarByClass(wordseg.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path("/user/rxy160030/assignment1"));
			FileOutputFormat.setOutputPath(job, new Path(args[0]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	

}
