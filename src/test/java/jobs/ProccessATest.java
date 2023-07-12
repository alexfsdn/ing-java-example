package jobs;

import base.services.SparkSessionsServices;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class ProccessATest {

    private SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();

    public void buildMock() {
        SQLContext hiveContext = spark.sqlContext();

        hiveContext.sql("CREATE DATABASE IF NOT EXISTS testDB");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS testDB.tableUser (\n" +
                "    name STRING,\n" +
                "    age STRING,\n" +
                "    cpf STRING,\n" +
                "    dat_ref STRING,\n" +
                "    TIME_STAMP_REFERENCE STRING,\n" +
                        "PARTITION_REFERENCE STRING" +
                ")\n");
                //"PARTITIONED BY (PARTITION_REFERENCE STRING)");

    }

    public void cleanUp() {
        spark.sqlContext().sql("DROP DATABASE IF EXISTS testDB CASCADE");
    }

    @Test
    public void test() {
        cleanUp();
        buildMock();

        String args[] = {"ProccessA"};
        JobRun.run(args, spark);

        spark.sqlContext().sql("select * from testDB.tableUser").show(20, false);

        String argsReproc[] = {"ProccessA", "20220812"};
        JobRun.run(argsReproc, spark);

        spark.sqlContext().sql("select * from testDB.tableUser").show(20, false);

        cleanUp();
    }


    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToJobName() {
        String args[] = {"ProccessNotExist"};
        JobRun.main(args);
    }

    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToDateInvalid() {
        String args[] = {"ProccessA", "123CantLettler"};
        JobRun.main(args);
    }
}
