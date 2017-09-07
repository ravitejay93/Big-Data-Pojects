package parti.parti;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class DownloadDataLink {
	
	public static void main(String[] args) {
		
		String srcFile = args[0];
		
		String dst = args[1];
		
		
		try {
			dst = make_directory(dst);
			ArrayList<String> link= get_download_link(srcFile);
			for(int i = 0; i < link.size();i++){
				get_file_link(link.get(i),dst);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
	}
	
	private static ArrayList<String> get_download_link(String srcFile) throws IOException{
		
		ArrayList<String> link = new ArrayList<String>();
		
		String line = null;
		
		BufferedReader b = new BufferedReader(new FileReader(srcFile));
		while((line=b.readLine())!= null){
			link.add(line);
			System.out.println(line);
		}		
		return link;
		
	}
	
	private static String get_file_link(String link,String dst) throws IOException{
		
		URL url = new URL(link);
		
		URLConnection con = url.openConnection();
		
		InputStream in = con.getInputStream();
		String fileName = "";
		
		for(int i = link.length()-1; i>=0;i--){
			if(link.charAt(i) == '/') break;
			fileName = link.charAt(i) + fileName;
			
		}		
		//get_file(in,out,1024);
		
		move_file_to_dir(in,dst+fileName);		
		return fileName;
	}
	private static void get_file(InputStream input,FileOutputStream output,int size) throws IOException{
		
		byte[] buf = new byte[size];
	    int n = input.read(buf);
	    while (n >= 0) {
	      output.write(buf, 0, n);
	      n = input.read(buf);
	    }
	    output.flush();
	}
	
	private static void move_file_to_dir(InputStream in,String dst) throws IOException{
			    
	    Configuration conf = new Configuration();
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	    FileSystem fs = FileSystem.get(URI.create(dst), conf);
	    OutputStream out = fs.create(new Path(dst), new Progressable() {
	      public void progress() {
	        System.out.print(".");
	      }
	    });
	    
	    IOUtils.copyBytes(in, out, 4096, true);
	    decompress_file(dst);
	}
	
	private static void decompress_file(String srcFile) throws IOException{
		String uri = srcFile;
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri);
	      System.exit(1);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	      fs.delete(new Path(srcFile),true);
	    } finally {
	      IOUtils.closeStream(in);
	      IOUtils.closeStream(out);
	    }
	    
	}
	
	private static String make_directory(String dst) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		if(!fs.exists(new Path(dst+"assignment1"))){
	    	fs.mkdirs(new Path(dst+"assignment1"));
	    	System.out.print(fs.getUri().toString()   );
	    	System.out.println("Creating Folder");
	    }
		dst = dst + "assignment1/";
		return dst;
	}
	

}
