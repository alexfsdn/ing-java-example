package jobs;

import base.services.SparkSessionsServices;
import model.enums.ProccessAEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class ProccessATest {

    private SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();

    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private final String PARTITION_REFERENCE = "PARTITION_REFERENCE";

    public void buildMock() {
        SQLContext hiveContext = spark.sqlContext();

        hiveContext.sql("CREATE DATABASE IF NOT EXISTS testDB");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS testDB.tableUser (\n" +
                ProccessAEnum.name.name() + " STRING, " +
                ProccessAEnum.age.name() + " STRING, " +
                ProccessAEnum.cpf.name() + " STRING, " +
                ProccessAEnum.dat_ref.name() + " STRING, " +
                TIME_STAMP_REFERENCE + " TIMESTAMP )" +
                "PARTITIONED BY (" + PARTITION_REFERENCE + " STRING)");

    }

    public void cleanUp() {
        spark.sqlContext().sql("DROP DATABASE IF EXISTS testDB CASCADE");
    }

    @Test
    public void test() {
        cleanUp();
        buildMock();

        String label_20230711 = "20230711";
        String args[] = {"ProccessA", label_20230711};
        JobRun.run(args, spark);

        spark.sqlContext().sql("select * from testDB.tableUser").show(20, false);

        String label_20220812 = "20220812";
        String argsReproc[] = {"ProccessA", label_20220812};
        JobRun.run(argsReproc, spark);

        JobRun.run(args, spark);//overwrite 20230711

        Dataset<Row> finalResult = spark.sqlContext().sql("select * from testDB.tableUser");

        finalResult.show(20, false);

        Dataset<Row> dt20230711 = finalResult.filter(PARTITION_REFERENCE.concat("=").concat(label_20230711));

        Dataset<Row> dt20220812 = finalResult.filter(PARTITION_REFERENCE.concat("=").concat(label_20220812));

        spark.sqlContext().sql("show partitions testDB.tableUser ").show(10, false);


        Assert.assertEquals(5, dt20230711.count());
        Assert.assertEquals(5, dt20220812.count());

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
