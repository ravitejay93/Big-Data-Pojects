package partofspeech.pos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class pos {
	
	public static HashMap<String, String> m = new HashMap();
	
	public static class TokenizerMapper extends Mapper<Object, Text,IntWritable,Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text in = new Text();
		
		public void setup(Mapper.Context context) throws IOException{
			generate_reference();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String val = itr.nextToken();
				if(val.length() >= 5){
					if(ispalindrome(val)) context.write(new IntWritable(val.length()),new Text("palindrome"));
					if(m.get(val)!=null){
						String s = m.get(val);
						if(get_name(s.charAt(0)) != null) context.write(new IntWritable(val.length()),new Text(get_name(s.charAt(0))));
					}
					
				}
			}
		}
		
		public static boolean ispalindrome(String val){
			
			int i = 0;
			int j = val.length()-1;
			
			while(i < j){
				if(val.charAt(i) != val.charAt(j)) return false;
			
				i++;
				j--;
			}
			
			return true;
		}
				
		public String get_name(char c){
			String result = null;
			if(c == 'N') result = "noun";
			if(c == 'p') result = "plural";
			if(c == 'h') result = "noun phrase";
			if(c == 'V') result = "verb";
			if(c == 't') result = "verb";
			if(c == 'i') result = "verb";
			if(c == 'A') result = "adjective";
			if(c == 'v') result = "adverb";
			if(c == 'C') result = "conjunction";
			if(c == 'P') result = "preposition";
			if(c == '!') result = "interjection";
			if(c == 'r') result = "pronoun";
			if(c == 'D') result = "definite article";
			if(c == 'I') result = "indefinite article";
			if(c == 'o') result = "nominative";
			
			return result;
		}

		public static void generate_reference() throws IOException{
			String line = null;
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader b = new BufferedReader(new InputStreamReader(fs.open(new Path("/user/rxy160030/input/mobyposi.i"))));
			//BufferedReader b = new BufferedReader(new InputStreamReader(new FileInputStream("mobyposi.i")));
			
			while((line=b.readLine())!= null){
				get_hash_table(line);
			}
						
		}
		public static void get_hash_table(String val){
			String s1 = "";
			String s2 = "";
			int i =0;
			for(i = 0;i < val.length();i++){
				byte b = (byte) val.charAt(i);
				if((int) b == -3) break;
				s1 += val.charAt(i);
			}
			for(i=i+1;i < val.length();i++){
				s2+=val.charAt(i);
			}
			if(s1.length()>=5) m.put(s1,s2);
		}
	}
	
	public static class IntSumReducer extends Reducer<IntWritable,Text,Text,Text> {
						
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			int palindrome = 0;
			HashMap<String,Integer> m =new HashMap<String, Integer>();
			for (Text val : values) {
				if(val.toString().equals("palindrome")){
					palindrome++;
				}
				else{
					if(m.containsKey(val.toString())){
						m.put(val.toString(),m.get(val.toString())+1);
					}
					else{
						m.put(val.toString(),1);
					}
					sum++;
				}
				
								
			}
			//result.set(sum);
			context.write(new Text("Length:"),new Text(String.valueOf(key.get())));
			context.write(new Text("Count of Words:"),new Text(String.valueOf(sum)));
			String result="";
			for(String name:m.keySet()){
				result += name;
				result += ":";
				result += String.valueOf(m.get(name).intValue());
				result += "; ";
			}
			context.write(new Text("Distribution of POS:"),new Text(result));
			context.write(new Text("Number of Palindromes:"),new Text(String.valueOf(palindrome)));
		}
	}
	
	public static void main(String[] args){
		
		try {
	    	Configuration conf = new Configuration();
		    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		    conf.set("mapreduce.framework.name", "yarn");
		    Job job = Job.getInstance(conf, "word_segmentation");
		    job.setJarByClass(pos.class);
		    
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setMapperClass(TokenizerMapper.class);
		    job.setReducerClass(IntSumReducer.class);
		    
		    
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
