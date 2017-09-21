package pt.uminho.haslab.testingutils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShareCluster {
	protected final List<MiniHBaseCluster> clusters;
	protected final List<HBaseAdmin> admins;
	protected final List<Configuration> configs;
	static final Log LOG = LogFactory.getLog(ShareCluster.class.getName());

	public ShareCluster(List<String> resources, int nRegionServers)
			throws Exception {
		clusters = new ArrayList<MiniHBaseCluster>();
		admins = new ArrayList<HBaseAdmin>();
		configs = new ArrayList<Configuration>();

		LOG.debug("Going to start start shareCluster " + resources);

		for (String resource : resources) {
			LOG.debug("Going to start minicluster " + resource);
			Configuration conf = HBaseConfiguration.create();
			conf.addResource(resource);

			MiniZooKeeperCluster zoo = new MiniZooKeeperCluster();
			int zooKeeperPort = conf.getInt("zookeeper.port", -1);
			String zooFile = conf.get("zookeeper.data.file");
			String[] rootDir = conf.get("hbase.rootdir").split("file://");

			FileUtils.deleteDirectory(new File(zooFile));
			FileUtils.deleteDirectory(new File(rootDir[1]));
			new File(rootDir[1]).mkdir();

			zoo.setDefaultClientPort(zooKeeperPort);
			zoo.startup(new File(zooFile));

			configs.add(conf);
			admins.add(new HBaseAdmin(conf));
			MiniHBaseCluster hbase = new MiniHBaseCluster(conf, nRegionServers);
			clusters.add(hbase);

		}

		LOG.info("Created " + clusters.size()
				+ " clusters. Going to wait for them to start");
		// wait for everything to be online, hbase and CMiddleware
		Thread.sleep(1000);
		LOG.info("Stopped waiting for cluster start");

	}

	public void tearDown() throws IOException {
		for (HBaseAdmin admin : admins) {
			LOG.info("Deleting tables");
			admin.disableTables(".*");
			admin.close();
		}
		for (MiniHBaseCluster cluster : clusters) {
			LOG.info("shuting down cloud");
			cluster.shutdown();
		}
	}

	public ClusterTables createTables(String tableName, String columnFamily)
			throws IOException {

		TableName tbname = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(columnFamily);
		table.addFamily(family);
		for (HBaseAdmin admin : admins) {
			admin.createTable(table);

		}

		return new ClusterTables(configs, tbname);
	}

	public boolean tableExists(String tableName) throws IOException {
		boolean tableExists = true;

		for (HBaseAdmin admin : admins) {
			tableExists &= admin.tableExists(tableName);

		}
		return tableExists;
	}

	public boolean mastersAreActive() {
		boolean isActive = true;
		for (MiniHBaseCluster cluster : clusters) {

			isActive &= cluster.getMaster().isActiveMaster();
		}
		return isActive;
	}
}
