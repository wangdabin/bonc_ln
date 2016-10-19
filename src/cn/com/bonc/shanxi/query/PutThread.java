package cn.com.bonc.shanxi.query;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.util.Bytes;

public class PutThread extends Thread {
	Log log = LogFactory.getLog(PutThread.class);
	LinkedBlockingQueue<Pair<Pair<String, String>, Long>> records;
	SimpleDateFormat format;
	Configuration conf;
	HConnection conn;
	HTableInterface[] table;
	int num;
	Info info;
	Info info1;
	String[] tableNames;
	Map<String, Pair<String, String>> tabAndKeys;

	MessageDigest md;

	int errURL;
	int errTEL;
	int allRecord = 0;
	int wh = 0;
	int correct = 0;
	Info status1;

	PutThread(Info info,
			LinkedBlockingQueue<Pair<Pair<String, String>, Long>> records,
			SimpleDateFormat format, Configuration conf, int num,
			String[] tableNames, Map<String, Pair<String, String>> tabAndKeys,
			Info status1) throws NoSuchAlgorithmException {
		// log.warn("----------------------------Thread is calling");log.warn("启动线程"
		log.warn("-----------------------start_thread");
		this.records = records;
		this.format = format;
		this.conf = conf;
		this.num = num;
		this.info = info;
		this.tableNames = tableNames;
		this.tabAndKeys = tabAndKeys;
		md = MessageDigest.getInstance("MD5");
		this.status1 = status1;
	}
	
	public static long errRecord = 0;
	
	public Map<String, Pair<byte[], byte[]>> getKeyValueNew(String[] line,
			Long putNum, String fileName) {
		/**
		 * 1:hash 2:% 3:&
		 * 
		 * 
		 */
		Map<String, Pair<byte[], byte[]>> kvs = new HashMap<String, Pair<byte[], byte[]>>();

		Pair<String, String> pair = tabAndKeys.get("ipsource");
		String s = pair.getSecond();

		String URL = "";

		byte[] rowkey = null;
		String mdn = line[1];
		String time = line[17];
		if(mdn==""){
			errRecord++;
		}
		rowkey = Utils.rk(mdn, time, putNum);

		for (String table : tableNames) {
			// if (table.endsWith("url_index") && line.length != 0) {
			if (line.length <= 31) {
				// 记录数小于30，所需字段没有，略过
				log.debug("length <= 31");
				kvs.put(table, null);
			}

			else if (line.length > 31) {

				// && ("1".equals(line[15].trim())
				// || "2".equals(line[15].trim())
				// || "7".equals(line[15].trim()) || "8"
				// .equals(line[15].trim()))
				if (table.endsWith("url_index")) {
					URL = line[5];
					
					byte[] url_hash = Bytes
							.toBytes((short) (URL.hashCode() & 0x7fff));
					byte[] url = Utils.url2byte(URL);
					byte[] time1 = Utils.time2byte(line[17]);
					byte[] mdn1 = Utils.mdn2byte(line[1]);
					byte[] num = Bytes.toBytes(putNum);
					byte[] digest = md.digest(Bytes.toBytes(URL));
					byte[] rk = Bytes.add(url_hash, digest, time1);
					rk = Bytes.add(rk, rowkey);
					// System.out.println(")))))");
					kvs.put(table,
							new Pair<byte[], byte[]>(rk, Bytes.toBytes("")));
				} else if (!table.endsWith("url_index")) {
					mdn = line[1];
					time = line[17];
					byte[] rk = rowkey;
					StringBuffer sb = new StringBuffer("");
					if (pair.getSecond() != "" ) {
						String[] value = pair.getSecond().split(",");

						for (String string : value) {
							if (Integer.parseInt(string) > line.length) {
								String string2 = "";
								sb.append(string2).append("|");
							} else {
								String string2 = line[Integer.parseInt(string)];
								sb.append(string2).append("|");
							}
						}

						byte[] v = Bytes.toBytes(sb.toString());

						// System.out.println(sb);
						kvs.put(table, new Pair<byte[], byte[]>(rowkey, v));
					} else if (pair.getSecond() != "" ) {
						String[] value = pair.getSecond().split(",");
						for (String string : value) {
							String string2 = line[Integer.parseInt(string)];
							sb.append(string2).append("|");
						}
						byte[] v = Bytes.toBytes(sb.toString());
						kvs.put(table, new Pair<byte[], byte[]>(rowkey, v));
					} else {
						log.debug("else-------------------");
						kvs.put(table, null);
					}
				}
			}
		}
		return kvs;

	}

	public Map<String, Pair<byte[], byte[]>> getKeyValue(String[] line,
			Long putNum) {
		/**
		 * 1:hash 2:% 3:&
		 * 
		 * 
		 */
		Map<String, Pair<byte[], byte[]>> kvs = new HashMap<String, Pair<byte[], byte[]>>();

		Pair<String, String> pair = tabAndKeys.get("ipsource");
		String s = pair.getSecond();
		// 获取入库所需的字段的最大的位置
		String[] split = s.split(",");
		int max = 0;
		if (split.length > 1 && split[0] != "") {
			max = Integer.parseInt(split[0]);
		}
		if (split.length > 2) {
			for (String string : split) {
				if (Integer.parseInt(string) > max) {
					max = Integer.parseInt(string);
				}
			}
		}
		String URL = "";

		byte[] rowkey = null;
		if (line.length > 17) {
			String mdn = line[1];
			String time = line[17];
			rowkey = Utils.rk(mdn, time, putNum);
		}

		for (String table : tableNames) {
			// if (table.endsWith("url_index") && line.length != 0) {
			if (line.length <= 31) {
				// 记录数小于30，所需字段没有，略过
				log.debug("length <= 31");
				kvs.put(table, null);
			}

			else if (line.length > 31) {

				// && ("1".equals(line[15].trim())
				// || "2".equals(line[15].trim())
				// || "7".equals(line[15].trim()) || "8"
				// .equals(line[15].trim()))
				if (table.endsWith("url_index")) {
					URL = line[29];
					byte[] url_hash = Bytes
							.toBytes((short) (URL.hashCode() & 0x7fff));
					byte[] url = Utils.url2byte(URL);
					byte[] time = Utils.time2byte(line[17]);
					byte[] mdn = Utils.mdn2byte(line[1]);
					byte[] num = Bytes.toBytes(putNum);
					byte[] digest = md.digest(Bytes.toBytes(URL));
					byte[] rk = Bytes.add(url_hash, digest, time);
					rk = Bytes.add(rk, rowkey);
					// System.out.println(")))))");
					kvs.put(table,
							new Pair<byte[], byte[]>(rk, Bytes.toBytes("")));
				} else if (!table.endsWith("url_index")) {
					String mdn = line[1];
					String time = line[17];
					byte[] rk = rowkey;
					StringBuffer sb = new StringBuffer("");
					if (pair.getSecond() != "" && line.length < max) {
						String[] value = pair.getSecond().split(",");

						for (String string : value) {
							if (Integer.parseInt(string) > line.length) {
								String string2 = "";
								sb.append(string2).append("|");
							} else {
								String string2 = line[Integer.parseInt(string)];
								sb.append(string2).append("|");
							}
						}

						byte[] v = Bytes.toBytes(sb.toString());

						// System.out.println(sb);
						kvs.put(table, new Pair<byte[], byte[]>(rowkey, v));
					} else if (pair.getSecond() != "" && line.length >= max) {
						String[] value = pair.getSecond().split(",");
						for (String string : value) {
							String string2 = line[Integer.parseInt(string)];
							sb.append(string2).append("|");
						}
						byte[] v = Bytes.toBytes(sb.toString());
						kvs.put(table, new Pair<byte[], byte[]>(rowkey, v));
					} else {
						log.debug("else-------------------");
						kvs.put(table, null);
					}
				}
			}
		}
		return kvs;

	}

	@Override
	public void run() {

		try {
			log.warn("-------获取connection");

			table = new HTableInterface[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				conn = HConnectionManager.createConnection(conf);
				table[i] = conn.getTable(tableNames[i]);
				table[i].setAutoFlush(false, false);
				table[i].setWriteBufferSize(8 * 1024 * 1024);
			}

			Map<String, List<Put>> puts = new HashMap<String, List<Put>>();
			for (int i = 0; i < tableNames.length; i++) {
				puts.put(tableNames[i], new ArrayList<Put>());
			}

			while (true) {
				Pair<Pair<String, String>, Long> poll = records.poll();

				if (poll == null && info.getBoolean()) {
					log.warn("-------死亡，flush---------");
					for (int i = 0; i < tableNames.length; i++) {
						table[i].put(puts.get(tableNames[i]));
						try {
							table[i].flushCommits();
						} catch (Exception e) {
							try {
								table[i].flushCommits();
								log.warn("retry flush");
							} catch (Exception e1) {
								log.warn("retry error");
								status1.setNumber(1);
								return;
							}
						} finally {
							status1.setBoolean(true);
						}

					}
					return;
				} else if (poll == null && !info.getBoolean()) {
					try {
						log.warn("-------对列为空，等待--------");
						sleep(500);
						continue;
					} catch (InterruptedException e) {
						e.printStackTrace();
						log.warn("wait thread is error");
					}
				} else {

					log.warn("-------循环获取数据  队列长度" + records.size());
					allRecord++;

					String line = poll.getFirst().getFirst();
					String fileName = poll.getFirst().getSecond();
					Long putNum = poll.getSecond();

					String[] split = line.split("\\|");

					Map<String, Pair<byte[], byte[]>> keyvalue = new HashMap<String, Pair<byte[], byte[]>>();

					// keyvalue = getKeyValue(split, putNum);

					keyvalue = getKeyValueNew(split, putNum, fileName);

					Put[] p = new Put[tableNames.length];

					for (int i = 0; i < tableNames.length; i++) {
						Pair<byte[], byte[]> pp = keyvalue.get(tableNames[i]);
						if (pp != null) {

							Put put = new Put(pp.getFirst());
							put.add(Bytes.toBytes("f"), Bytes.toBytes("q"),
									pp.getSecond());
							put.setDurability(Durability.SYNC_WAL);
							synchronized (table[i]) {
								table[i].put(put);
							}

							correct++;
							// System.out.println("++");
						} else if (pp == null
								&& tableNames[i].endsWith("_index")) {
							errURL++;
							continue;
						} else if (pp == null
								&& !tableNames[i].endsWith("_index")) {
							errTEL++;
							continue;
						} else {
							wh++;
						}

					}

					// log.warn("--------------------");
					if (allRecord % num == 0) {
						int retry = 0;
						for (int i = 0; i < tableNames.length; i++) {
							table[i].put(puts.get(tableNames[i]));
							try {
								table[i].flushCommits();
								// log.warn("flush---------");
							} catch (Exception e) {
								try {
									table[i].flushCommits();
									log.warn("retry");
								} catch (Exception e1) {
									log.warn("retry error");
									continue;
								}

							}

							synchronized (puts.get(tableNames[i])) {
								puts.get(tableNames[i]).clear();
							}
						}
						// log.warn("--------auto flush");

					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			log.warn("write thread is error");
		} finally {
			log.warn("errorURL---" + errURL);
			log.warn("errorSource---" + errTEL);
			log.warn("all record" + allRecord);
			log.warn("正确:" + correct / 2);
			log.warn("未知:" + wh);
			try {
				for (HTableInterface t : table) {
					if (t != null) {
						t.close();
					}
				}
				if (conn != null) {
					conn.close();
				}
			} catch (IOException e) {
				info1.setBoolean(true);
				e.printStackTrace();
			}
		}
	}

}
