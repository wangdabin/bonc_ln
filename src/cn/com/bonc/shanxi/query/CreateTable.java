package cn.com.bonc.shanxi.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

	public static void main(String[] args) {
		if(args.length<5){
			System.out.println("zk-hosts /hbase 10 ipsource cf");
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", args[0]);
		conf.set("zookeeper.znode.parent", args[1]);
		HBaseAdmin admin;
		boolean tableExists;
		try {
			int regionNum = Integer.parseInt(args[2]);
			short start = (short) (0x7FFF / regionNum);
			short end = (short) (0x7FFF - start);
			
			admin = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(args[3]));
			HColumnDescriptor hd = new HColumnDescriptor(Bytes.toBytes(args[4]));
			hd.setMaxVersions(1);
			hd.setBlocksize(256*1024);
			hd.setTimeToLive(Integer.MAX_VALUE);
			hd.setBloomFilterType(BloomType.NONE);
			hd.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
			hd.setCompactionCompressionType(Algorithm.LZ4);
			desc.addFamily(hd);
			admin.createTable(desc,Bytes.toBytes(start),Bytes.toBytes(end),regionNum);
			System.out.println("table:"+args[3] +" has already created.Column Family is "+args[4] +". the num of region is "+args[2]);
		} catch (MasterNotRunningException e) {
			System.out.println("!!!error");
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			System.out.println("!!!error");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("!!!error");
			e.printStackTrace();
		}
	}
}
