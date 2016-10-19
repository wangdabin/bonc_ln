package cn.com.bonc.shanxi.query;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
	public static void main(String[] args) {
//		String s = "460110264316336|8618050525511|8679980255893500|ctlte.MNC011.MCC460.GPRS|106.39.255.184|80|10.39.145.220|59011|115.169.100.142|115.169.100.2|115.169.129.155||64F0111104|64F011|6|4|949|20160121065452|20160121065500|7758|7231870|176160|5027|2742|155941823219503164|2|45090000000000000000000000000000000000000000||Mozilla/5.0 (Linux; U; Android 5.0.2; zh-cn; NX510J Build/LRX22G) AppleWebKit/533.1 (KHTML, like Gecko)Version/4.0 MQQBrowser/5.4 TBS/025489 Mobile Safari/533.1 V1_AND_SQ_6.1.0_312_YYB_D QQ/6.1.0.2635 NetType/4G WebP/0.3.0 Pixel/1080|us.sinaimg.cn/002Rwe4qjx06Yl8aCfaf01040101H5230k02.mp4?KID=unistore,video&Expires=1453334003&ssig=MobNunZyOe|sinaimg.cn|us.sinaimg.cn|||0||6||9|0";
//		String[] split = s.split("\\|");
//		if (split.length >= 32) {
//			String up = split[20];
//			String down = split[21];
//			long parseLong = Long.parseLong(up);
//			long parseLong2 = Long.parseLong(down);
//			long g = parseLong + parseLong2;
//			long a = 2 * 1024 * 1024;
//			if (split[1] != null && split[1] != "" && split[1] != "\\N"
//					&& split.length >= 32 && g >= a) {
//				System.out.println("yes");
//			}
//		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
		System.out.println(simpleDateFormat.format(new Date()));
	}
}
