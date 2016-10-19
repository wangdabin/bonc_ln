package cn.com.bonc.shanxi.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

/**
 * 
 * 
 * 从队列中获取文件名，并读取文件到记录队列中，
 * 
 * 当文件列表队列为空后，将INFO设置为true 标识该读线程已经工作完成
 * 
 * @author wk
 *
 */
public class ReadRecord{
	//记录队列，《记录值，行偏移量》
	LinkedBlockingQueue<Pair<Pair<String,String>,Long>> queue;
	//已经入库的文件
	Set<String> alreadyPutFile;
	String dir;
	FileSystem fs;
	Configuration conf;
	static Log log = LogFactory.getLog(ReadRecord.class);
	Info info;
	//待入库文件列表
	LinkedBlockingQueue<LocatedFileStatus> fileList;
	FSDataInputStream open;
	InputStreamReader isr ;
	BufferedReader br;
	String queueSize;
	static FileWriter fw = null;
	static FileWriter errfw = null;
	static BufferedWriter bw = null;
	static BufferedWriter errbw = null;
	String LOGPATH;
	String ERRORLOGPATH;
	InputStream is;
	GZIPInputStream gzis;
	Info status;
	static SimpleDateFormat simpleDateFormat;
	HashMap<String, String> flag;
	
	
	public ReadRecord(SimpleDateFormat simpleDateFormat,Info info, Configuration conf,
			LinkedBlockingQueue<Pair<Pair<String,String>,Long>> queue, String dir, FileSystem fs,
			LinkedBlockingQueue<LocatedFileStatus> fileList,String queueSize,String LOGPATH,String ERRORLOGPATH,Info Status,HashMap<String, String> flag) throws FileNotFoundException {
		File file = new File(LOGPATH);
		File file1 = new File(ERRORLOGPATH);
		try {
			fw = new FileWriter(file,true);
			errfw = new FileWriter(file1,true);
		} catch (IOException e) {
			log.warn("logfile error---------------");
			e.printStackTrace();
		}
		this.simpleDateFormat = simpleDateFormat;
		bw = new BufferedWriter(fw);
		errbw = new BufferedWriter(fw);
		this.flag = flag;
		this.queue = queue;
		this.dir = dir;
		this.alreadyPutFile = alreadyPutFile;
		this.fs = fs;
		this.fileList = fileList;
		this.conf = conf;
		this.info = info;
		this.queueSize = queueSize;
		this.LOGPATH = LOGPATH;
		this.ERRORLOGPATH = ERRORLOGPATH;
	}
	
	// 记录已经入库文件
	public static void writeLogForPut(String name) {
		//System.out.println("记录文件----------------");
			try {
				synchronized (ReadRecord.class) {
					bw.write(simpleDateFormat.format(new Date())+"\t"+name);
					bw.newLine();
					bw.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
				log.warn("----------write put log for " +name + " error" );
			}
		} 
	public static void writeErrorLogForPut(String name) {
		//System.out.println("记录文件----------------");
			try {
				synchronized (ReadRecord.class) {
					errbw.write(simpleDateFormat.format(new Date())+"\t"+name);
					errbw.newLine();
					errbw.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
				log.warn("----------write put log for " +name + " error" );
			}
		} 

	public void run() {
		while(true){
//			synchronized (info) {
//				info.setBoolean(false);
//			}
			long putNum = 0L;
			
			LocatedFileStatus fileName = fileList.poll();
			if (fileName == null) {
				synchronized (status) {
					status.setBoolean(true);
					info.setBoolean(true);
					status.setNumber(0);
				}
				return;
			} 
			
			String name = fileName.getPath().getName();
			
			 if (name.endsWith(".gz")) {
				//	log.warn("-------------开始读文件");
					try {
						if (fs == null) {
							fs = FileSystem.get(conf);
						}
						open = fs.open(new Path(dir+ name),8192);
//						gzis = new GZIPInputStream(open, 8192);
//						isr = new InputStreamReader(gzis);
						isr = new InputStreamReader(open);
						br = new BufferedReader(isr);
						String line = "";
						while ((line = br.readLine()) != null) {
							String[] split = line.split("\\|");
							long g = 0;
							if(split.length>=22){
								String up = split[20];
								String down = split[21];
								long parseLong = Long.parseLong(up);
								long parseLong2 = Long.parseLong(down);
								g = parseLong+parseLong2;
								long a = 2*1024*1024;
								if(split[1]!=null&&split[1]!=""&&split[1]!="\\N"&&g>=a){
								long queuesize = queue.size();
								
								while(queuesize>Long.parseLong(queueSize)){
//									System.out.println("sleep");
									Thread.sleep(500);
									queuesize = queue.size();
								}
								queue.add(new Pair(new Pair(line, name),
										putNum));}
							}
							
						}
						// 标识已经读完
						//log.warn(fileName+"文件读完");
						flag.put(dir+name, "T");
						System.out.println("文件读完");
					} catch (Exception  e ) {
						log.warn("read thread exception"+name+"");
						flag.put(dir+name, "F");
						e.printStackTrace();
						continue;
					} finally{
//						log.warn("read thread close");
						try {
						if(br!=null){
								br.close();
							} 
						if(isr!=null){
							isr.close();
						}if(gzis!=null){
							gzis.close();
						}if(open!=null){
							open.close();
						}
							
						}catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			else{
					System.out.println("文件名"+fileName+"非法");
					log.warn("文件名"+fileName+"非法");
					continue;
				}

		}
		
			}
		}

