package pt.uminho.haslab.testingutils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.client.Result;

public class ScanValidator {

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

		while (stopIndex < selectedStartIndex) {
			stopIndex = rand.nextInt(vals.size());
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

		valid = valuesInRange.size() == results.size();

		for (Result res : results) {
			byte[] resValue = res.getRow();
			valid &= valueSet.contains(new BigInteger(resValue));
		}
		return valid;
	}
        
        
	private List<BigInteger> getKeysInRange() {
		List<BigInteger> rangeValues = new ArrayList<BigInteger>();

		for (int i = 0; i < vals.size(); i++) {
			BigInteger val = vals.get(i);

			boolean greaterOrEqualThan = val.compareTo(this.selectedStartKey) == 0
					|| val.compareTo(this.selectedStartKey) == 1;
			boolean lessOrEqualThan = val.compareTo(this.selectedStopKey) == -1;

			if (greaterOrEqualThan && lessOrEqualThan) {
				rangeValues.add(val);
			}
		}

		return rangeValues;

	}
}
