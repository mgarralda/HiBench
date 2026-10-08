package HiBench;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Dictionary support retained for Bayes and Nutch generators. */
public class RawData {
    private static final String dict = "/words";
	public static int putDictToHdfs(Path hdfs_dict, int size) throws IOException {

		Utils.checkHdfsPath(hdfs_dict);
		
		FileSystem fs = hdfs_dict.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_dict);

		InputStream is=new RawData().getClass().getResourceAsStream(dict);
		int len = 0;
		if (is!=null) {
			
			InputStreamReader isr=new InputStreamReader(is);
                        BufferedReader br = new BufferedReader(isr);	
			
			while (len < size) {
				String word = br.readLine() + "\n";
//				if (null == word) break;
				
				fout.write(word.getBytes("UTF-8"));
				len++;
			}
			br.close();
		}
		fout.close();
		return len;
	}
}
