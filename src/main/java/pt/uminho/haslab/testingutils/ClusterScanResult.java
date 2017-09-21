package pt.uminho.haslab.testingutils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smhbase.interfaces.SharedSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

public class ClusterScanResult {
	static final Log LOG = LogFactory.getLog(ClusterScanResult.class.getName());

	private final List<List<Result>> scans;

	private byte[] columnFamily;
	private byte[] columnQualifier;
	private int nbits;

	public ClusterScanResult(List<List<Result>> scansResults) {
		this.scans = scansResults;
	}

	public void setColumnFamily(byte[] columnFamily) {
		this.columnFamily = columnFamily;
	}

	public void setColumnQualifier(byte[] columnQualifier) {
		this.columnQualifier = columnQualifier;
	}

	public void setNbits(int nbits) {
		this.nbits = nbits;
	}

	public boolean isConsistant() {
		List<Result> scanOne = scans.get(0);
		List<Result> scanTwo = scans.get(1);
		List<Result> scanThree = scans.get(2);
		LOG.debug("Scan size is " + scanOne.size());
		return scanOne.size() == scanTwo.size()
				& scanTwo.size() == scanThree.size();

	}

	public List<Integer> getOriginalResult() {
		List<Integer> results = new ArrayList<Integer>();

		for (Result result : scans.get(0)) {
			results.add(Integer.parseInt(new String(result.getRow())));
		}
		return results;
	}

	public List<List<Result>> getAllResults() {
		return scans;
	}

	public List<Result> getDecodedResults() throws IOException {

		List<Result> res = new ArrayList<Result>();

		for (int j = 0; j < scans.get(0).size(); j++) {

			Result res0 = scans.get(0).get(j);
			Result res1 = scans.get(1).get(j);
			Result res2 = scans.get(2).get(j);

			byte[] res0_id = res0.getRow();
			byte[] res1_id = res1.getRow();
			byte[] res2_id = res2.getRow();

			if (!(Arrays.equals(res0_id, res1_id) && Arrays.equals(res1_id,
					res2_id))) {
				throw new IllegalStateException(
						"Cluster scan results rows ids are not matching");
			}

			BigInteger res0_val = new BigInteger(res0.getValue(columnFamily,
					columnQualifier));
			BigInteger res1_val = new BigInteger(res1.getValue(columnFamily,
					columnQualifier));
			BigInteger res2_val = new BigInteger(res2.getValue(columnFamily,
					columnQualifier));

			SharedSecret secret = new SharemindSharedSecret(nbits, res0_val,
					res1_val, res2_val);
			LOG.debug("Result has value " + secret.unshare());
			CellScanner scanner = res0.cellScanner();
			List<Cell> cells = new ArrayList<Cell>();

			while (scanner.advance()) {
				Cell cell = scanner.current();

				if (!(Arrays.equals(CellUtil.cloneFamily(cell), columnFamily) && Arrays
						.equals(CellUtil.cloneFamily(cell), columnQualifier))) {
					byte[] columnFamily = CellUtil.cloneFamily(cell);
					byte[] columnQualifier = CellUtil.cloneQualifier(cell);
					long timestamp = cell.getTimestamp();
					byte type = cell.getTypeByte();
					byte[] value = CellUtil.cloneValue(cell);

					Cell nCell = CellUtil.createCell(secret.unshare()
							.toByteArray(), columnFamily, columnQualifier,
							timestamp, type, value);
					cells.add(nCell);
				}

			}
			res.add(Result.create(cells));
		}

		return res;
	}

	public boolean notEmpty() {
		boolean result = true;

		for (List<Result> results : scans) {
			result &= !results.isEmpty();
			for (Result res : results) {
				result &= !res.isEmpty();
			}
		}

		return result;

	}
}
