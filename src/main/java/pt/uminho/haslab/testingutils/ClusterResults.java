package pt.uminho.haslab.testingutils;

import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public class ClusterResults {

	protected final List<Result> results;

	public ClusterResults(List<Result> resp) {

		this.results = resp;
	}

	public List<Result> getResults() {
		return results;
	}

	public Result getResult(int i) {
		return results.get(i);
	}

	public boolean allEmpty() {

		boolean empty = true;

		for (Result res : results) {
			empty &= res.isEmpty();
		}

		return empty;
	}

	public boolean oneEmpty() {
		boolean empty = false;

		for (Result res : results) {

			empty |= res.isEmpty();
		}
		return empty;
	}

	public boolean isInconsistant() {
		return oneEmpty() ^ allEmpty();

	}

}
