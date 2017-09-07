package partii.partii;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.tools.ant.taskdefs.Zip;
import org.apache.tools.zip.ZipEntry;

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
		
		ZipInputStream zin = new ZipInputStream(in);
		
		java.util.zip.ZipEntry nze = zin.getNextEntry();
		
		String fileName = nze.getName();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		
		OutputStream out = fs.create(new Path(dst+fileName));
		
		get_file(zin,out,1024);
		
		return fileName;
	}
	private static void get_file(ZipInputStream input,OutputStream output,int size) throws IOException{
		
		byte[] buf = new byte[size];
	    int n = input.read(buf);
	    while (n >= 0) {
	      output.write(buf, 0, n);
	      n = input.read(buf);
	    }
	    output.flush();
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
