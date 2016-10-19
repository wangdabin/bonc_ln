package cn.com.bonc.shanxi.bulkload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class HbasePut {
	public static class ConvertFile2HFile extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws java.io.IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			
			
			//创建HBase中的RowKey
			byte[] rowkey = Bytes.toBytes("");
			ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkey);
			byte[] cf = Bytes.toBytes("cf");
			byte[] q = Bytes.toBytes("q");
			byte[] hbaseValue = Bytes.toBytes("");
			Put put = new Put(rowkey);
			put.add(cf, q, hbaseValue);
			context.write(immutableBytesWritable, put);
		};
	}
}
