package pt.uminho.haslab.testingutils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;

public class ScanValidator {

	static final Log LOG = LogFactory.getLog(ScanValidator.class.getName());

	private final List<BigInteger> vals;

	private BigInteger selectedStartKey;

	private final Random rand;

	private int selectedStartIndex;

	private BigInteger selectedStopKey;

	public ScanValidator(List<BigInteger> values) {
		this.vals = values;
		selectedStartKey = null;
		selectedStopKey = null;
		rand = new Random();
		sortValues();
	}

	public byte[] generateStartKey() {
		selectedStartIndex = rand.nextInt(vals.size());
		selectedStartKey = vals.get(selectedStartIndex);
		return selectedStartKey.toByteArray();
	}

	public byte[] generateStopKey() {
		int stopIndex = 0;

		if (selectedStartIndex == 0) {
			stopIndex = rand.nextInt(vals.size());
		} else {
			while (stopIndex < selectedStartIndex) {
				stopIndex = rand.nextInt(vals.size());
			}
		}

		selectedStopKey = vals.get(stopIndex);
		return selectedStopKey.toByteArray();
	}

	private void sortValues() {
		Collections.sort(vals, new Comparator<BigInteger>() {
			public int compare(BigInteger value1, BigInteger value2) {
				return value1.compareTo(value2);
			}
		});

	}

	public boolean validateResults(List<Result> results) {
		boolean valid;
		List<BigInteger> valuesInRange = getKeysInRange();

		Set<BigInteger> valueSet = new HashSet<BigInteger>(valuesInRange);
		LOG.debug("Values in range " + valuesInRange + " cenas " + results);
		valid = valuesInRange.size() == results.size();

		for (Result res : results) {
			byte[] resValue = res.getRow();
			BigInteger val = new BigInteger(resValue);
			LOG.debug("Receive value is " + val);
			valid &= valueSet.contains(val);
		}
		return valid;
	}

	private List<BigInteger> getKeysInRange() {
		List<BigInteger> rangeValues = new ArrayList<BigInteger>();

		LOG.debug("Selected start key is " + this.selectedStartKey);
		LOG.debug("Selected stop key is " + this.selectedStopKey);

		for (int i = 0; i < vals.size(); i++) {
			BigInteger val = vals.get(i);
			LOG.debug("Value stored is " + val);

			/**
			 * default case is when no key is generated and every value is valid
			 */
			boolean valid = true;

			if (this.selectedStartKey != null && this.selectedStopKey != null) {
				boolean greaterOrEqualThan = val
						.compareTo(this.selectedStartKey) == 0
						|| val.compareTo(this.selectedStartKey) == 1;
				boolean lessOrEqualThan = val.compareTo(this.selectedStopKey) == -1;

				valid = greaterOrEqualThan && lessOrEqualThan;
			} else if (this.selectedStartKey == null
					&& this.selectedStopKey != null) {
				boolean lessOrEqualThan = val.compareTo(this.selectedStopKey) == -1;
				valid = lessOrEqualThan;
			} else if (this.selectedStartKey != null
					&& this.selectedStopKey == null) {
				boolean greaterOrEqualThan = val
						.compareTo(this.selectedStartKey) == 0
						|| val.compareTo(this.selectedStartKey) == 1;
				valid = greaterOrEqualThan;
			}

			if (valid) {
				LOG.debug("Going to add value " + val);
				rangeValues.add(val);
			}

		}

		return rangeValues;

	}
}
