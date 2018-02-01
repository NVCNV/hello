package cn.com.dtmobile.hadoop.getActiveHost;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

public class ActiveNNAdd {
	
	
	
	public void getNameNodeAdress() throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem system = null;
		try {
			system = FileSystem.get(conf);
			InetSocketAddress active = HAUtil.getAddressOfActive(system);
			System.out.println(active.getHostString()); 
//			System.out.println("hdfs port:" + active.getPort());
//			InetAddress address = active.getAddress();
//			System.out.println("hdfs://" + address.getHostAddress() + ":"+ active.getPort()); 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (system != null) {
					system.close();
				}
			} catch (IOException e) {
			e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) {
		
		
		ActiveNNAdd nn = new ActiveNNAdd() ;
		
		try {
			nn.getNameNodeAdress();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
}
