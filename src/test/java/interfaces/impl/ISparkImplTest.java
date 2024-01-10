package interfaces.impl;

import base.services.SparkSessionsServices;
import interfaces.ISpark;
import model.enums.ProcessAEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

public class ISparkImplTest {

    private SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();


    @Test
    public void getDatasetTest() {
        ISpark iSpark = new ISparkImpl(spark);
        Map<String, String> map = new HashMap<>();
        map.put("delimiter", ";");
        map.put("header", "true");
        map.put("encoding", "UTF-8");
        map.put("modo", "DROPMALFORMED");

        StructType structType = ProcessAEnum.schema();

        Dataset<Row> ds = iSpark.getDataset("src/test/resources/input/process_a_20220812.csv", "csv", map, structType);

        ds.show(10, false);

    }

    @Test
    public void saveAsTableTest() {
        spark.sqlContext().sql("DROP TABLE IF EXISTS default.tablename");
        ISpark iSpark = new ISparkImpl(spark);
        Map<String, String> map = new HashMap<>();
        map.put("delimiter", ";");
        map.put("header", "true");
        map.put("encoding", "UTF-8");
        map.put("modo", "DROPMALFORMED");

        StructType structType = ProcessAEnum.schema();

        Dataset<Row> ds = iSpark.getDataset("src/test/resources/input/process_a_20230711.csv", "csv", map, structType);
        Dataset<Row> ds2 = iSpark.getDataset("src/test/resources/input/process_a_20220812.csv", "csv", map, structType);

        ds = ds.withColumn("data_particao", lit("20240110"));
        ds2 = ds2.withColumn("data_particao", lit("20240109"));
        Dataset<Row> ds3 = ds.withColumn("data_particao", lit("20240110"));

        Dataset<Row> dsToSave = ds.union(ds2).union(ds3).distinct();

        iSpark.saveAsTable(dsToSave, "default.tablename", "orc", "data_particao");

        spark.sql("select * from default.tablename limit 50").show(50, false);
        spark.sql("show partitions default.tablename").show(50, false);
    }
}
