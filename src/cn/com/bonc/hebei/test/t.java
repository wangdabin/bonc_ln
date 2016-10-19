package cn.com.bonc.hebei.test;

import cn.com.bonc.shanxi.query.Utils;

public class t {
	public static void main(String[] args) {
		String s = "460110056856270|8613373064920||ctlte.mnc011.mcc460.gprs|124.238.238.49|80|10.37.24.224|50939|||||||9|4|96|20160122002532|20160122002532|6|4269|1001|6|4|158471323203470970||||Mozilla/5.0 (Linux; Android 4.4.2; Coolpad T1 Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36 baiduboxapp/5.0 (Baidu; P1 4.4.2)|wa3.baidu-1img.cn/timg?appsite&quality=80&size=b180_180&imgtype=4&sec=1453393526&di=fdd022b2fe1dc3350702de3b48239877&src=http%3A%2F%2Fa.hiphotos.bdimg.com%2Fwisegame%2Fpic%2Fitem%2F6f338744ebf81a4c99c2c473d22a6059242da69c.jpg|baidu-1img.cn|wa3.baidu-1img.cn|3412|image/webp|1|http://mobile.baidu.com/|6|200|6|0      0";
		String line = s.toString();
		String[] split = line.split("\\|");
		if (split.length >= 22) {
			String up = split[20];
			String down = split[21];
			long parseLong = Long.parseLong(up);
			long parseLong2 = Long.parseLong(down);
			long g = parseLong + parseLong2;
			long a = 2 * 1024 * 1024;
			if (split[1] != null && split[1] != "" && split[1] != "\\N"
					&& g >= a) {
				if (split.length > 31) {
					try {
						String URL = split[29];
						String TIME = split[17];
						String MDN = split[1];
						byte[] url = Utils.url2byte(URL);
						byte[] time = Utils.time2byte(TIME);
						byte[] mdn = Utils.mdn2byte(MDN);
					} catch (Exception e) {
						System.out.println("eee");
					}}else{
						System.out.println("3");
					}
	}else{
		System.out.println("2");
	}
			}else{
			System.out.println("1");
	}
}}
