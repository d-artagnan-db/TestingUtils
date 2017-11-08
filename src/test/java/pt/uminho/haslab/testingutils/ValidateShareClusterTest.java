package pt.uminho.haslab.testingutils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ValidateShareClusterTest {
    static final Log LOG = LogFactory.getLog(ValidateShareClusterTest.class
            .getName());

    protected final ShareCluster cluster;
    protected final int maxBits;
    protected final List<BigInteger> values;

    public ValidateShareClusterTest(int maxBits, List<BigInteger> values)
            throws Exception {

        List<String> resources = new ArrayList<String>();

        for (int i = 1; i < 4; i++) {
            resources.add("hbase-site-" + i + ".xml");

        }

        cluster = new ShareCluster(resources, 1);
        this.maxBits = maxBits;
        this.values = values;
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.SingleListValuesGenerator();
    }

    @After
    public void tearDown() throws IOException {
        cluster.tearDown();
    }

    @Test
    public void testMasterActive() {
        assertEquals(true, cluster.mastersAreActive());
    }

    @Test
    public void testCreateTables() throws IOException {
        LOG.debug("Going to create table");
        assertEquals(false, cluster.tableExists("teste"));
        LOG.debug("Going to create table");
        cluster.createTables("teste", "teste");
        LOG.debug("Table created");
        assertEquals(true, cluster.tableExists("teste"));

    }

    private ClusterTables createTableAddValues(String tableName, String cf,
                                               String cq) throws IOException {

        ClusterTables tables = cluster.createTables(tableName, cf);
        int id = 0;

        byte[] columnFamilyBytes = cf.getBytes();
        byte[] columnQualifierBytes = cq.getBytes();

        for (BigInteger value : values) {
            byte[] key = ("" + id).getBytes();

            Put put = new Put(key);
            put.add(columnFamilyBytes, columnQualifierBytes,
                    value.toByteArray());
            tables.put(put);
            id++;
        }

        return tables;
    }

    @Test
    public void testPutGet() throws IOException {

        ClusterTables tables = createTableAddValues("testPutGet", "teste",
                "teste");
        int id = 0;
        for (BigInteger value : values) {
            byte[] key = ("" + id).getBytes();

            Get get = new Get(key);
            id++;
            ClusterResults res = tables.get(get);
            byte[] cf = "teste".getBytes();

            for (Result result : res.getResults()) {
                byte[] getValue = result.getValue(cf, cf);
                BigInteger receivedResult = new BigInteger(getValue);
                Assert.assertEquals(receivedResult, value);
            }
        }
    }

    @Test
    public void testScan() throws IOException, InterruptedException {

        ClusterTables tables = createTableAddValues("testScan", "teste",
                "teste");
        byte[] cf = "teste".getBytes();

        Scan scan = new Scan();
        List<List<Result>> results = tables.scan(scan).getAllResults();
        for (BigInteger value : values) {
            int found = 0;
            for (List<Result> clusterResults : results) {
                for (Result res : clusterResults) {
                    byte[] getValue = res.getValue(cf, cf);
                    BigInteger scanValue = new BigInteger(getValue);
                    if (scanValue.equals(value))
                        found++;

                }
            }
            Assert.assertEquals(3, found);
        }

    }

}
