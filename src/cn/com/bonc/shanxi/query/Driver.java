package cn.com.bonc.shanxi.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

//import com.sun.xml.bind.Util;

public class Driver {
	private static Log log = LogFactory.getLog(Driver.class);
	private static FileSystem fs;
	private static String dir;
	private static LinkedBlockingQueue<String> fileList;
	private static File hdfs = new File(
			"/opt/beh/core/hadoop/etc/hadoop/hdfs-site.xml");
	private static File core = new File(
			"/opt/beh/core/hadoop/etc/hadoop/core-site.xml");
	private static File hbase = new File(
			"/opt/beh/core/hbase/conf/hbase-site.xml");
	private static InputStream is;
	private static InputStream is1;
	private static InputStream is2;
	private static String LOGPATH;
	private static HashSet<String> alreadyPutFile;
	private static SimpleDateFormat format;
	private static Configuration HDFS_CONF = new Configuration();
	private static Configuration HBASE_CONF = HBaseConfiguration.create();
	private static int proNum;
	private static Configuration[] CONFS ;
	private static String[] TABLENAMES;
	private static String zks;
	static File file ;
	static PrintWriter pw = null;
	static BufferedWriter bw = null;
	private static File lockFile;
	/**
	 * 当天日期(YYYYMMDD)   HDFS目录     TableFlush_size   写线程个数    zk地址
	 */
	public static void main(String[] args) {
		try {
			SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
//			String d = f.format(DateUtils.addDays(new Date(), -Integer.parseInt(args[0])));
		    String d = args[0];    //当天日期   YYYYMMDD
//			String d = args[4];
			lockFile = new File("/tmp/isPuting"+d+".lock");
			if(lockFile.exists()){
				System.exit(0);
			}else{
				lockFile.createNewFile();
			String[] split = "ipsource,ipsource_url_index".split(",");
			TABLENAMES = new String[split.length];
			for(int i = 0 ;i <split.length;i++){
				TABLENAMES[i] = split[i];
			}
			CONFS = new Configuration[TABLENAMES.length];
//			dir = "hdfs://beh/DPI/"+(Long.parseLong(d))+"/";
			dir = args[1];   //hdfs目录
			//System.out.println("dir : "+dir);
//			final int nn = 20000;
			final int nn = Integer.parseInt(args[2]);
			//写
//			int threadNum = Integer.parseInt("6");
			int threadNum = Integer.parseInt(args[3]);
			//读
//			proNum = Integer.parseInt("6");
//			proNum = Integer.parseInt(args[4]);
			proNum = Integer.parseInt(args[4]);
			
			//LOGPATH = "/opt/beh/logs/putresult/"+args[7];
			LOGPATH = "/opt/beh/logs/putresult/"+(Long.parseLong(d))+".log";
			zks = args[5];
			//String queueSize = args[6];
			String queueSize = "1000000";
			//获取CONF、表名、rowkey、value
			//getConfigurationAndTableKeys();
			
//			File file = new File(LOGPATH);
//			pw = new PrintWriter(file);
//			bw = new BufferedWriter(pw);

			is = new FileInputStream(hdfs);
			is1 = new FileInputStream(core);
			is2 = new FileInputStream(hbase);
			HDFS_CONF.addResource(is1);
			HDFS_CONF.addResource(is);
			fs = FileSystem.get(HDFS_CONF);

			HBASE_CONF.setInt("hbase.client.retries.number", 5);
			HBASE_CONF.setInt("hbase.meta.scanner.caching", 5000);
			HBASE_CONF.setInt("hbase.client.prefetch.limit", 100);
			HBASE_CONF.setLong("zookeeper.session.timeout", 900000);
//			HBASE_CONF.set("hbase.zookeeper.quorum",
//					"hadoop-m01,hadoop-m02,hadoop-d07");
			HBASE_CONF.set("hbase.zookeeper.quorum", zks);
			//获取文件列表
			fileList = getFileList();
			
			//获取已经入库文件
			alreadyPutFile = getAlreadyPut();
			System.out.println("已入库文件数----"+alreadyPutFile.size());
			//表 rowkey value映射关系
			Map<String,Pair<String,String>> tabAndKeys =new HashMap<String, Pair<String,String>>();
//			tabAndKeys.put(args[4], new Pair<>("","18,19,27,15,14,4,5,12,13,16,20,21,9,25"));
//			tabAndKeys.put(args[5], new Pair<>("",""));
			
			tabAndKeys.put("ipsource", new Pair<String,String>("",Utils.getProperties("fields")));
			tabAndKeys.put("ipsource_url_index", new Pair<String,String>("",""));
			
			
//			ExecutorService exec = Executors.newFixedThreadPool(proNum);
//			ExecutorService exec1 = Executors.newFixedThreadPool(32);
			for (int j = 0; j < proNum; j++) {
				

				Thread[] threads = new Thread[threadNum];
				LinkedBlockingQueue<Pair<String, Long>> readFromHDFSFile = new LinkedBlockingQueue<Pair<String, Long>>();
				Info info = new Info();
				try {
					
					Thread readRecord = new ReadRecord(info, HDFS_CONF,
							readFromHDFSFile, alreadyPutFile, dir, fs, fileList,queueSize,LOGPATH);
					readRecord.start();
					
//					startReadRecord(info, HDFS_CONF,
//							readFromHDFSFile, alreadyPutFile, dir, fileList);
					
					for (int i = 0; i < threadNum; i++) {
						
						 
						format = new SimpleDateFormat("YYYYMMddHHmmss");
					threads[i] = new PutThread(info,readFromHDFSFile, format, HBASE_CONF, nn,TABLENAMES,tabAndKeys);
						
//						threads[i] = startWriteRecord(info1,info, tableName,
//							readFromHDFSFile, format, HBASE_CONF, nn);
//						threads[i].setDaemon(true);
						threads[i].start();
					}
					
//					Thread readRecord = new ReadRecord(info, HDFS_CONF,
//							readFromHDFSFile, alreadyPutFile, dir, fs, fileList);
					
					//readRecord.setDaemon(true);
//					exec.execute(readRecord);
					//readRecord.start();
				} catch (Exception e) {
					lockFile.deleteOnExit();
					e.printStackTrace();
					log.warn("------Exception read and put-----");
				}
			}
			
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				
				@Override
				public void run() {
					lockFile.deleteOnExit();
				}
			}));
			
//			exec.shutdown();
//			exec1.shutdown();
			
			// for(int i = 0 ; i < 8;i++){
			// try {
			// threads[i].join();
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			// }
			
//			TableFlush tableFlush = new TableFlush(tables);
//			tableFlush.setDaemon(true);
//			tableFlush.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
			lockFile.deleteOnExit();
		}
	}
	
	
	
	
//	public static void getConfigurationAndTableKeys(){
//		if(tabAndKeys.size()==0){
//			log.warn("please set tablename");
//			return;
//		}
//		for(int i = 0 ; i < tabAndKeys.size();i++){
//			CONFS[i] = HBaseConfiguration.create();
//			CONFS[i].setInt("hbase.client.retries.number", 5);
//			CONFS[i].setInt("hbase.client.retries.number", 5);
//			CONFS[i].setInt("hbase.meta.scanner.caching", 5000);
//			CONFS[i].setInt("hbase.client.prefetch.limit", 100);
//			CONFS[i].setLong("zookeeper.session.timeout", 900000);
//			CONFS[i].set("hbase.zookeeper.quorum",ZKNAME);
//			
//			List<String> list = tabAndKeys.get(TABLENAMES[i]);
//			if(list.size()==0){
//				log.info("please set rowkey");
//				return;
//			}
//			
//			StringBuffer sb = new StringBuffer("");
//			Iterator<String> iterator = list.iterator();
//			String rowKey = iterator.next();
//			while(iterator.hasNext()){
//				sb.append(iterator.next()).append("|");
//			}
//			
//		}
//	}
	
//	public static HTable[] getTables(){
//		HTable[] tables = new HTable[TABLENAMES.length];
//		for(int i = 0 ; i <TABLENAMES.length;i++){
//			try {
//				tables[i] = new HTable(CONFS[i], TABLENAMES[i]);
//				tables[i].setAutoFlush(false, false);
//				tables[i].setWriteBufferSize(1024 * 1024 * 8);
//			} catch (IOException e) {
//				e.printStackTrace();
//				log.warn("exception in getTables");
//			}
//		}
//		return tables;
//	}
	
	
    /**
     * 读取已经导入的文件
     */
	public static HashSet<String> getAlreadyPut() {
		System.out.println("获取已入库文件");
		BufferedReader br = null;
		FileReader fr = null;
		File file = null;
		HashSet<String> alreadyPut = new HashSet<String>();
		try {
			file = new File(LOGPATH);
			if(!file.exists()){
				System.out.println("创建log文件-------------");
				file.createNewFile();
			}else{
				fr = new FileReader(new File(LOGPATH));
				br = new BufferedReader(fr);
				String line = "";
				while ((line = br.readLine()) != null) {
					alreadyPut.add(line);
				}
			}
			

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			
				try {
					if(br!=null){
					br.close();
					}
					if(fr!=null){
						fr.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return alreadyPut;
	}

	// 获取文件内容
	// 文件名--->(行内容--->putNum)

	// 获取文件列表,加入全局的队列，为读线程工作
	// 将要读取的hdfs文件加入到queue队列中
	public static LinkedBlockingQueue<String> cz() {
		RemoteIterator<LocatedFileStatus> listFiles;
		LinkedBlockingQueue<String> fileList = new LinkedBlockingQueue<String>();
		try {
			listFiles = fs.listFiles(new Path(dir), false);
			FileStatus f = null;
			while (listFiles.hasNext()) {
				LocatedFileStatus next = listFiles.next();
				 //log.warn("------------文件列表"+next);
				fileList.add(next.getPath().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileList;
	}


	
	
	

}



class TableFlush extends Thread{
	Vector<HTable> tables;

	TableFlush(Vector<HTable> tables){
		this.tables = tables;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				sleep(5000l);
				if(tables.size()==0){
					return;
				}
				synchronized (tables) {
					Iterator<HTable> iterator = tables.iterator();
					while(iterator.hasNext()){
						HTable next = iterator.next();
						synchronized (next) {
							try {
								next.flushCommits();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}