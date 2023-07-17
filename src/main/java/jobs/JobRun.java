package jobs;

import base.services.SparkSessionsServices;
import interfaces.IProccess;
import org.apache.spark.sql.SparkSession;
import utils.TodayUtils;

import java.net.URISyntaxException;


public class JobRun {

    public static void main(String[] args) {
        try {
            SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();
            new JobRun().run(args, spark);
        } catch (URISyntaxException uriSyntaxException) {
            uriSyntaxException.getMessage();
        }
    }

    public static void run(String[] args, SparkSession spark) throws URISyntaxException {
        if (args.length <= 0) {
            throw new IllegalArgumentException(String.format("argument with job name is required"));
        }

        String jobName = args[0].trim();
        String dt_ref = "";
        String logStarting = "";

        if (args.length > 1) {
            dt_ref = args[1].trim();
            if (!dt_ref.matches("\\d+")) {
                throw new IllegalArgumentException(String.format("The %s argument is invalid, the second argument should type date valid ", dt_ref));
            }
            logStarting = "Execution reproccess data to job %s and date %s is starting...";
        } else {
            dt_ref = TodayUtils.getTodayOnlyNumbers();
            logStarting = "Execution proccess data to job %s and date %s is starting...";
        }

        System.out.println(String.format(logStarting, jobName, dt_ref));

        IProccess job = new JobList().job(jobName);

        job.run(spark, dt_ref);
    }
}
