package interfaces;

import org.apache.spark.sql.SparkSession;

public interface IProccess {

    void run(SparkSession spark, String dt_ref);
}
