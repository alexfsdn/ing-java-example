package jobs;

import base.services.SparkSessionsServices;
import model.enums.ProcessAEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;

public class ProcessATest {

    private SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();

    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private final String PARTITION_REFERENCE = "dat_reference";

    public void buildMock() {
        SQLContext hiveContext = spark.sqlContext();

        hiveContext.sql("CREATE DATABASE IF NOT EXISTS testDB");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS testDB.tableUser (\n" +
                ProcessAEnum.name.name() + " STRING, " +
                ProcessAEnum.age.name() + " STRING, " +
                ProcessAEnum.cpf.name() + " STRING, " +
                ProcessAEnum.dat_ref.name() + " STRING, " +
                TIME_STAMP_REFERENCE + " TIMESTAMP )" +
                "PARTITIONED BY (" + PARTITION_REFERENCE + " STRING)");

    }

    public void cleanUp() {
        spark.sqlContext().sql("DROP DATABASE IF EXISTS testDB CASCADE");
    }

    @Test
    public void test() throws URISyntaxException {
        cleanUp();
        buildMock();

        String label_20230711 = "20230711";
        String args[] = {"ProcessA", label_20230711};
        JobRun.run(args, spark);

        spark.sqlContext().sql("select * from testDB.tableUser").show(20, false);

        String label_20220812 = "20220812";
        String argsReproc[] = {"ProcessA", label_20220812};
        JobRun.run(argsReproc, spark);

        JobRun.run(args, spark);//overwrite 20230711

        Dataset<Row> finalResult = spark.sqlContext().sql("select * from testDB.tableUser");

        /*
        +---------+---+-----------+--------+-----------------------+-------------+
        |name     |age|cpf        |dat_ref |TIME_STAMP_REFERENCE   |dat_reference|
        +---------+---+-----------+--------+-----------------------+-------------+
        |Alexandre|31 |11111111111|20230811|2023-07-27 12:02:22.25 |20230711     |
        |Alana    |32 |22222222222|20230812|2023-07-27 12:02:22.25 |20230711     |
        |Bruno    |33 |33333333333|20230813|2023-07-27 12:02:22.25 |20230711     |
        |Carlos   |34 |44444444444|20230814|2023-07-27 12:02:22.25 |20230711     |
        |Daniel   |35 |55555555555|20230814|2023-07-27 12:02:22.25 |20230711     |
        |Alex     |31 |11111111111|20220811|2023-07-27 12:02:20.229|20220812     |
        |Aline    |32 |22222222222|20220812|2023-07-27 12:02:20.229|20220812     |
        |Bruna    |33 |33333333333|20220813|2023-07-27 12:02:20.229|20220812     |
        |Carla    |34 |44444444444|20220814|2023-07-27 12:02:20.229|20220812     |
        |Daniela  |35 |55555555555|20220814|2023-07-27 12:02:20.229|20220812     |
        +---------+---+-----------+--------+-----------------------+-------------+
         */

        finalResult.show(20, false);

        Dataset<Row> dt20230711 = finalResult.filter(PARTITION_REFERENCE.concat("=").concat(label_20230711));

        Dataset<Row> dt20220812 = finalResult.filter(PARTITION_REFERENCE.concat("=").concat(label_20220812));

        spark.sqlContext().sql("show partitions testDB.tableUser ").show(10, false);

        /*      +----------------------+
                |partition             |
                +----------------------+
                |dat_reference=20220812|
                |dat_reference=20230711|
                +----------------------+
         */

        Assert.assertEquals(5, dt20230711.count());
        Assert.assertEquals(5, dt20220812.count());

        cleanUp();
    }


    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToJobName() {
        String args[] = {"ProcessNotExist"};
        JobRun.main(args);
    }

    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToDateInvalid() {
        String args[] = {"ProcessA", "123CantLettler"};
        JobRun.main(args);
    }
}
