package interfaces.impl;

import interfaces.ISpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class ISparkImpl implements ISpark {


    private SparkSession spark;

    public ISparkImpl(SparkSession spark) {
        if (spark == null) {
            throw new IllegalArgumentException();
        }

        this.spark = spark;
    }

    @Override
    public SparkSession getSpark() {
        return this.spark;
    }

    @Override
    public void save(List<String> columns, String tableName, String tableNameTmp, String partitionName, String partition) {
        try {

            String sqlCommand = String.format(
                    "INSERT OVERWRITE TABLE %s PARTITION (%s=%s)\n" +
                            "SELECT %s\n" +
                            "FROM %s",
                    tableName, partitionName, partition, String.join(",", columns), tableNameTmp
            );

            spark.sql(sqlCommand);

        } catch (Exception e) {
            System.err.println("Erro inesperado: " + e.getMessage());
            throw e;
        }
    }

    /***
     * Read path and return a type Dataset
     * @param pathFileName
     * @param format
     * @param map
     * @param schema
     * @return
     */
    @Override
    public Dataset<Row> getDataset(String pathFileName, String format, Map<String, String> map, StructType schema) {
        Dataset<Row> ds = spark.read().format(format).options(map).option("mode", "PERMISSIVE") //The PERMISSIVE mode sets to null field values when corrupted records are detected. By default, if you don't specify the parameter mode, Spark sets the PERMISSIVE value.
                .schema(schema).load(pathFileName).cache();
        return ds;
    }
}
