package pt.uminho.haslab.testingutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ClusterTables {

	static final Log LOG = LogFactory.getLog(ClusterTables.class.getName());

	protected final List<Configuration> configs;
	protected final TableName tname;
	protected final List<HTable> tables;

	public ClusterTables(List<Configuration> configs, TableName tbname)
			throws IOException {
		this.configs = configs;
		this.tname = tbname;
		tables = new ArrayList<HTable>();

		for (Configuration config : configs) {
			tables.add(new HTable(config, tbname));
		}

	}

	public ClusterTables put(Put put) throws IOException {

		for (HTable table : tables) {
			table.put(put);
		}
		return this;
	}

	public ClusterResults get(Get get) throws IOException {
		List<Result> results = new ArrayList<Result>();

		for (HTable table : tables) {
			results.add(table.get(get));

		}

		return new ClusterResults(results);

	}

	private class ConcurrentGet extends Thread {

		private final Get get;
		private final HTable table;
		private Result result;

		public ConcurrentGet(HTable table, Get get) {
			this.get = get;
			this.table = table;
		}

		public Result getResult() {
			return result;
		}

		@Override
		public void run() {
			try {
				result = table.get(get);
			} catch (IOException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}

		}

	}

	private class ConcurrentScan extends Thread {

		private final Scan scan;
		private final HTable table;
		private ResultScanner scanner;
		private final List<Result> results;

		public ConcurrentScan(HTable table, Scan scan) {
			this.scan = scan;
			this.table = table;
			results = new ArrayList<Result>();
		}

		public List<Result> getResults() {
			return results;
		}

		@Override
		public void run() {
			try {
				scanner = table.getScanner(scan);
				LOG.debug("Concurrent scanner is running");
				for (Result result = scanner.next(); result != null; result = scanner
						.next()) {
					LOG.debug("Going to add scan result " + result);
					results.add(result);
				}

			} catch (IOException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}
			LOG.debug("Scan result size is " + results.size());

		}

	}

	public ClusterResults get(List<Get> gets) throws IOException,
			InterruptedException {
		List<Result> results = new ArrayList<Result>();
		List<ConcurrentGet> tgets = new ArrayList<ConcurrentGet>();

		for (int i = 0; i < gets.size(); i++) {
			HTable table = tables.get(i);
			Get get = gets.get(i);
			tgets.add(new ConcurrentGet(table, get));
		}

		for (ConcurrentGet t : tgets) {
			t.start();
		}

		for (ConcurrentGet t : tgets) {

			t.join();

		}

		for (ConcurrentGet t : tgets) {

			results.add(t.getResult());
		}

		return new ClusterResults(results);

	}

	public ClusterTables put(int clusterID, Put put) throws IOException {
		tables.get(clusterID).put(put);
		return this;

	}

	public ClusterScanResult scan(List<Scan> scans) throws IOException,
			InterruptedException {

		List<ConcurrentScan> tscans = new ArrayList<ConcurrentScan>();
		List<List<Result>> results = new ArrayList<List<Result>>();

		for (int i = 0; i < scans.size(); i++) {
			HTable table = tables.get(i);
			Scan scan = scans.get(i);
			LOG.debug("Creating new Concurrent Scan");
			tscans.add(new ConcurrentScan(table, scan));
		}

		for (ConcurrentScan t : tscans) {
			LOG.debug("Launching concurent Scans");

			((Thread) t).start();
		}

		for (ConcurrentScan t : tscans) {
			LOG.debug("Joining conccurent scans");
			((Thread) t).join();

		}

		for (ConcurrentScan t : tscans) {

			results.add(t.getResults());
		}

		return new ClusterScanResult(results);
	}

	public ClusterScanResult scan(Scan scan) throws IOException,
			InterruptedException {
		List<ConcurrentScan> tscans = new ArrayList<ConcurrentScan>();
		List<List<Result>> results = new ArrayList<List<Result>>();
		for (int i = 0; i < 3; i++) {
			HTable table = tables.get(i);
			tscans.add(new ConcurrentScan(table, scan));
		}

		for (ConcurrentScan t : tscans) {
			((Thread) t).start();
		}

		for (ConcurrentScan t : tscans) {

			((Thread) t).join();

		}

		for (ConcurrentScan t : tscans) {

			results.add(t.getResults());
		}

		return new ClusterScanResult(results);
	}
}
