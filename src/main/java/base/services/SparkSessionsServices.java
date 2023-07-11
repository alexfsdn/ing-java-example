package base.services;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionsServices {

    public static SparkSession devLocal() {
        try {
            SparkConf conf = new SparkConf().setAppName("App Name example dev")
                    .set("hive.exec.dynamic.partition.mode", "nonstrict")
                    .set("spark.some.config.option", "some-value");

            SparkSession sparkSession = SparkSession.builder().master("local[2]")
                    .appName("spark local")
                    .config(conf)
                    .getOrCreate();

            return sparkSession;

        } catch (RuntimeException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SparkSession devLocalEnableHiveSupport() {
        try {
            SparkConf conf = new SparkConf().setAppName("App Name example prod")
                    .set("hive.exec.dynamic.partition.mode", "nonstrict")
                    .set("spark.some.config.option", "some-value")
                    .set("spark.sql.catalogImplementation", "hive");

            SparkSession sparkSession = SparkSession.builder().master("local[2]")
                    .appName("spark local")
                    .config(conf)
                    .getOrCreate();

            return sparkSession;

        } catch (RuntimeException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static SparkSession prd() {
        try {
            SparkConf conf = new SparkConf().setAppName("App Name example prod")
                    .set("hive.exec.dynamic.partition.mode", "nonstrict")
                    .set("spark.some.config.option", "some-value");

            SparkSession sparkSession = SparkSession.builder().master("yarn")
                    .appName("spark prd yarn")
                    .config(conf)
                    .enableHiveSupport()
                    .getOrCreate();

            return sparkSession;

        } catch (RuntimeException e) {
            e.printStackTrace();
            return null;
        }
    }
}
