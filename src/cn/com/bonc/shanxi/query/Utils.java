package cn.com.bonc.shanxi.query;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;

public class Utils
{
	
	static int dayN = 24 * 3600;
	static int yearN = 400 * dayN;
	static int monthN = 32 * dayN;
	// static int dayN=24*3600;
	static int hourN = 3600;
	static int mmN = 60;
	
	
	public static byte[] time2byte(String date) {
		if (date == null)
			return null;
		date = date.trim();
		int year = Integer.parseInt(date.substring(0, 4)) - 2000;
		int month = Integer.parseInt(date.substring(4, 6));
		int day = Integer.parseInt(date.substring(6, 8));
		int hour = Integer.parseInt(date.substring(8, 10));
		int mm = Integer.parseInt(date.substring(10, 12));
		int ss = Integer.parseInt(date.substring(12, 14));
		return Bytes.toBytes(year * yearN + month * monthN + day * dayN + hour * hourN + mm
				* mmN + ss);
	}
	
	
	
  public static byte[] mdn2byte(String mdn) {
	  byte[] bytes = null;
	  try{
		 bytes  = Bytes.toBytes(Long.parseLong(mdn));
	  }
	  catch (Exception e) {
		  System.out.println(mdn+"+++++++++mdn error");
	}
    return bytes;
  }
  public static byte[] url2byte(String url) {
    return Bytes.toBytes(url);
  }
  public static byte[] rkHash(String mdn) {
    return Bytes
			.toBytes((short) (mdn.hashCode() & 0x7fff));
  }
  public static byte[] rk(String mdn,String time,Long putNum){
	  byte[] rowkey_hash = Bytes
				.toBytes((short) (mdn.hashCode() & 0x7fff));
	  //手机号为空
	  if(mdn!=""&&mdn!="\\N"){
		  try{
		  byte[] msisdn = Bytes.toBytes(Long.parseLong(mdn));
			byte[] t = time2byte(time);
			byte[] number = Bytes.toBytes(putNum);
			byte[] key = Bytes.add(rowkey_hash, msisdn, t);
			key = Bytes.add(key, number);
			return key;
			}catch (Exception e) {
				System.out.println(mdn+"------tel error------");
			}
	  }
	return rowkey_hash;
  }
  
  public static String getProperties(String key) throws IOException{
	  InputStream resourceAsStream = new FileInputStream(new File("/opt/beh/conf/fields.proeprties"));
	  Properties pro  = new Properties();
	  pro.load(resourceAsStream);
	  String  value = (String)pro.get(key);
	return value;
  }
}