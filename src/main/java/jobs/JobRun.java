package jobs;

import java.time.LocalDate;

public class JobRun {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        if (args.length <= 0) {
            throw new IllegalArgumentException(String.format("argument with job name is required"));
        }

        String jobName = args[0].trim();
        String dt_ref = "";
        String logStarting = "";

        if (args.length > 1) {
            dt_ref = args[1].trim();
            logStarting = "Execution reproccess data to job %s and date %s is starting...";
        } else {
            dt_ref = LocalDate.now().toString();
            logStarting = "Execution proccess data to job %s and date %s is starting...";
        }

        System.out.println(String.format(logStarting, jobName, dt_ref));

        IProccess job = new JobList().job(jobName);

        job.run(dt_ref);

    }
}
