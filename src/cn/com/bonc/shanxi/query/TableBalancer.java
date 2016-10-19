package cn.com.bonc.shanxi.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableBalancer {

	public static void main(String args[]) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum", "pezy1");

		conf.set("zookeeper.znode.parent", args[0]);
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor dess[] = admin.listTables();
		for (HTableDescriptor des : dess) {
			balanceTable(des.getNameAsString(), admin);
		}
		admin.close();

	}

	public static Random rand = new Random();

	public static List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {

		List<RegionPlan> ret = new ArrayList<RegionPlan>();
		if (clusterState == null || clusterState.size() == 0) {
			return ret;
		}
		while (true) {
			int minRegions = Integer.MAX_VALUE;
			int maxRegions = -1;
			ServerName largestF = null;
			ServerName minF = null;
			for (Entry<ServerName, List<HRegionInfo>> name : clusterState
					.entrySet()) {
				if (name.getValue().size() > maxRegions) {
					maxRegions = name.getValue().size();
					largestF = name.getKey();
				}
				if (name.getValue().size() < minRegions) {
					minRegions = name.getValue().size();
					minF = name.getKey();
				}
			}
			if (Math.abs(maxRegions - minRegions) < 2) {
				return ret;
			}

			int index = rand.nextInt(clusterState.get(largestF).size());
			HRegionInfo hri = clusterState.get(largestF).get(index);
			ret.add(new RegionPlan(hri, largestF, minF));
			clusterState.get(largestF).remove(index);
			clusterState.get(minF).add(hri);

		}

	}

	public static void balanceTable(String tableName, HBaseAdmin admin)
			throws IOException {
		Configuration conf = HBaseConfiguration.create();
		System.out.println("begain to balance:" + tableName);

		HRegionLocation loc = admin.getConnection().getRegionLocation(
				TableName.META_TABLE_NAME, null, false);
		ServerName meServer = loc.getServerName();
		Collection<ServerName> ser = admin.getClusterStatus().getServers();
		// servers.remove(meServer);

		List<ServerName> servers = new ArrayList<ServerName>();
		servers.addAll(ser);
		servers.remove(meServer);

		byte[] table = Bytes.toBytes(tableName);
		Map<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(
				conf, admin.getConnection(), TableName.valueOf(table), false);
		List<ServerName> se = new ArrayList<ServerName>();
		se.addAll(servers);
		System.out.println("move meta servers:" + regions.size() + " meta is:"
				+ meServer);
		for (Entry<HRegionInfo, ServerName> e : regions.entrySet()) {
			ServerName to = se.get(rand.nextInt(se.size()));

			if (e.getValue().equals(meServer) && !e.getKey().isMetaRegion()) {
				admin.move(e.getKey().getEncodedNameAsBytes(),
						Bytes.toBytes(to.getServerName()));
				// System.out.println("move:"+e.getKey().getRegionNameAsString()+" to:"+to);
			}
		}
		System.out.println("move meta servers finished");

		Map<ServerName, List<HRegionInfo>> assignment = null;
		assignment = new HashMap<ServerName, List<HRegionInfo>>();
		for (Entry<HRegionInfo, ServerName> e : regions.entrySet()) {
			if (servers.contains(e.getValue())) {
				if (assignment.get(e.getValue()) == null) {
					assignment.put(e.getValue(), new ArrayList<HRegionInfo>());
				}
				assignment.get(e.getValue()).add(e.getKey());
			} else {
				// If this regions are assign to wrong servers (maybe default
				// group check didn't find
				// this regions), reassign them.
				// wrongAssignment.put(e.getKey(), e.getValue());
			}
		}
		// Add available but null server
		for (ServerName server : servers) {
			if (assignment.get(server) == null) {
				assignment.put(server, new ArrayList<HRegionInfo>());
			}
		}

		List<RegionPlan> plans = balanceCluster(assignment);
		for (RegionPlan p : plans) {
			System.out.println(p);
			admin.move(p.getRegionInfo().getEncodedNameAsBytes(),
					Bytes.toBytes(p.getDestination().getServerName()));
		}
	}
}
