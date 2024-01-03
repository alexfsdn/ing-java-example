package interfaces;

import org.apache.spark.sql.SparkSession;

import java.net.URISyntaxException;

public interface IProcess {

    void run(SparkSession spark, String dt_ref) throws URISyntaxException;
}
