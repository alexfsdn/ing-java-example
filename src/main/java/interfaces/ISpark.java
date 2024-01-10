package interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public interface ISpark {

    SparkSession getSpark();

    /***
     * Save data to internal table
     *
     * @param ds
     * @param tableName
     * @param format
     * @param partitionName
     */
    default void saveAsTable(Dataset<Row> ds, String tableName, String format, String partitionName) {
        try {
            ds.write().format(format).mode(SaveMode.Overwrite).partitionBy(partitionName).
                    saveAsTable(tableName);
        } catch (Exception e) {
            System.err.println("Error unexpected: " + e.getMessage());
            throw e;
        }
    }

    /***
     * Save data to internal table
     *
     * @param ds
     * @param tableName
     * @param format
     * @param partitionName
     */
    default void save(Dataset<Row> ds, String tableName, String format, String partitionName) {
        try {
            ds.write().format(format).mode(SaveMode.Overwrite).partitionBy(partitionName)
                    .save(tableName);
        } catch (Exception e) {
            System.err.println("Error unexpected: " + e.getMessage());
            throw e;
        }
    }

    void save(List<String> columns, String tableName, String tableNameTmp, String partitionName, String partition);

    Dataset<Row> getDataset(String pathFileName, String format, Map<String, String> map, StructType schema);
}
