package interfaces;

import org.apache.spark.sql.SparkSession;

import java.net.URISyntaxException;

public interface IProccess {

    void run(SparkSession spark, String dt_ref) throws URISyntaxException;
}
