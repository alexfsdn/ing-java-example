package connections;

import base.services.SparkSessionsServices;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.apache.spark.sql.functions.current_timestamp;

public class SparkPostgresSQL {

    private SparkSession spark = SparkSessionsServices.devLocal();
    private String url = null;
    private Properties props = null;


    @Before
    public void setup() {
        this.url = "jdbc:postgresql://localhost:5432/andromeda";
        this.props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        props.setProperty("driver", "org.postgresql.Driver");

    }

    @Test
    public void simplesTest() {
        Dataset<Row> ds = spark.read().jdbc(url, "spark.usuario", props);

        ds.show(10, false);

        spark.stop();
    }

    @Test
    public void saveTest() {
        Dataset<Row> ds = spark.read().jdbc(url, "spark.usuario", props);

        ds = ds.withColumn("dat_ref_time", current_timestamp());

        ds.write().mode(SaveMode.Append)
                .jdbc(url, "spark.example", props);

        spark.stop();
    }
}
