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
        hiveContext.sql("CREATE TABLE IF NOT EXISTS testDB.tableUser (name STRING, age STRING, cpf STRING, dat_ref STRING, INTESTION_NAME STRING, PARTITION_NAME STRING)");

    }

    public void cleanUp() {
        spark.sqlContext().sql("DROP DATABASE IF EXISTS testDB CASCADE");
    }

    @Test
    public void testReproccess() {

       // cleanUp();
       // buildMock();

        String args[] = {"ProccessA", "20220812"};
        JobRun.main(args);
    }

    @Test
    public void test() {
        String args[] = {"ProccessA"};
        JobRun.main(args);
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
