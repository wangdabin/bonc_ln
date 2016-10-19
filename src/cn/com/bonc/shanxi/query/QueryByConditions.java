package cn.com.bonc.shanxi.query;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.org.apache.commons.logging.Log;


public class QueryByConditions {
	static org.apache.commons.logging.Log log =  LogFactory.getLog(QueryByConditions.class);
	static Configuration HBASE_CONF;
	static byte[] i_url;
	static byte[] url_hash;
	static byte[] stime;
	static MessageDigest md;
	
	///./queryByCondition.sh 'http://mmbiz.qpic.cn/mmbiz/4eqV8xE6icpJpt5uQR8jHUgepKfTLCpNpS7Vpa5BraibFNrlgLJVDyicmLtYc5Llfaia2AXbpIMBk1qcm5JnlE1ujg/640?wxfmt=jpeg&wxfrom=5' 1 20150419235017 20150419235018
	
	
	//.
	
	
	
	//./queryByCondition.sh 17703505733 1000 20150418235239 20150428105559 tel
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		System.out.println("-----");
		String flag = args[4];
		if(flag.length()==1){
			// 手机号   查询条数   开始时间  结束时间
			List<String[]> queryByTel = queryByTel(args[0], args[1], args[2], args[3], "ipsource");
		}if(flag.length()==2){
			md = MessageDigest.getInstance("MD5");
			//url  查询条数  开始时间  结束时间
			List<String[]> queryByURL = queryByURL(args[0],args[1],args[2],args[3],"ipsource","ipsource_url_index");
		}
		
		
	}
	
	public static List<String[]> queryByTel(String mdn,String limit,String startTime,String endTime,String tableName){
		log.warn("query by tel");
		System.out.println("tel"); 	
		if(mdn==null||"".equals(mdn)){
			log.warn("check your condition");
			return null;
		}
		HBASE_CONF = HBaseConfiguration.create();
		
		HBASE_CONF.setInt("hbase.client.retries.number", 5);
		HBASE_CONF.setInt("hbase.meta.scanner.caching", 5000);
		HBASE_CONF.setInt("hbase.client.prefetch.limit", 100);
		HBASE_CONF.setLong("zookeeper.session.timeout", 900000);
		HBASE_CONF.set("hbase.zookeeper.quorum", "hadoop-m01,hadoop-m02,hadoop-m03");
		HConnection conn;
		ArrayList<String[]>   records = new ArrayList<String[]>();
		try {
			conn = HConnectionManager.createConnection(HBASE_CONF);
			HTableInterface table2 = conn.getTable(tableName);
			Scan scan = new  Scan();
			 byte[] rowkey_hash = Utils.rkHash(mdn);
			 byte[] msisdn = Bytes.toBytes(Long.parseLong(mdn));
			 byte[] st = Utils.time2byte(startTime);
			 byte[] et = Utils.time2byte(endTime);
			 
			byte[] sk = Bytes.add(rowkey_hash, msisdn,st);
			byte[] ek = Bytes.add(rowkey_hash, msisdn,et);
			

			 
			scan.setStartRow(sk);
			scan.setStopRow(ek);
			
			ResultScanner scanner = table2.getScanner(scan);
			Result rs = null;
			int data = 0;
			
			
			while((rs = scanner.next())!=null){
				byte[] value = rs.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
				String v = new String(value);
				String[] split = v.split("\\|");
				if(records.size()<Integer.parseInt(limit)){
					records.add(split);
					data++;
				}else{
					break;
				}
				
				
				
			}
			
			System.out.println("记录数："+data);
		} catch (IOException e) {
			log.warn("query mdn exception");
			e.printStackTrace();
		}
		
		
		return records;
		
		
		
		
	}
	
	
	public static List<String[]> queryByURL(String url,String limit,String startTime,String endTime,String tableName,String table_index) throws IOException, NoSuchAlgorithmException{
		log.warn("query by url");
		System.out.println("url");
		if(url==null||"".equals(url)){
			log.warn("check your condition");
			return null;
		}
		md = MessageDigest.getInstance("MD5"); 
		url_hash = Bytes.toBytes((short) (url.hashCode() & 0x7fff));
				i_url = Utils.url2byte(url);
				stime = Utils.time2byte(startTime);
				byte[] etime = Utils.time2byte(endTime);
				byte[] digest = md.digest(Bytes.toBytes(url));
				byte[] sk = Bytes.add(url_hash, digest,stime);
				byte[] ek = Bytes.add(url_hash, digest,etime);
				
				HBASE_CONF = HBaseConfiguration.create();
				
				HBASE_CONF.setInt("hbase.client.retries.number", 5);
				HBASE_CONF.setInt("hbase.meta.scanner.caching", 5000);
				HBASE_CONF.setInt("hbase.client.prefetch.limit", 100);
				HBASE_CONF.setLong("zookeeper.session.timeout", 900000);
				HBASE_CONF.set("hbase.zookeeper.quorum", "hadoop-m01,hadoop-m02,hadoop-m03");
				HConnection conn = HConnectionManager.createConnection(HBASE_CONF);
				
				HTableInterface table = conn.getTable(Bytes.toBytes(table_index));
				
				HTableInterface table2 = conn.getTable(tableName);
				ArrayList<String[]> records = new ArrayList<String[]>();
				Scan scan = new  Scan();
				
				scan.setStartRow(sk);
				scan.setStopRow(ek);
				ResultScanner scanner = table.getScanner(scan);
				int n = 0;
				int data  =0;
				Result rs = null;
				
				List<byte[]> keys = new ArrayList(); 
				
				while((rs = scanner.next())!=null){
					int length = url_hash.length;
					byte[] row = rs.getRow();
					byte[] copyOfRange = Arrays.copyOfRange(row, length+digest.length+stime.length, row.length);
					
					if(keys.size()<Integer.parseInt(limit)){
						keys.add(copyOfRange);
						n++;
					}else{
						break;
					}
					
				}
				Iterator<byte[]> iterator = keys.iterator();
				while(iterator.hasNext()){
					Get get = new Get(iterator.next());
					Result result = table2.get(get);
					if(result!=null){
						byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
						System.out.println(new String(value));
						String v = new String(value);
						String[] split = v.split("\\|");
						records.add(split);
						data++;
					}
					
				}
				
				
				
				
		
				System.out.println("记录数"+n);
				System.out.println("data:"+data);
				return records;
		
		
	}
}
