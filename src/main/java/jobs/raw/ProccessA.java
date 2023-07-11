package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;
import jobs.IProccess;

@Partitioned(daily = true, partition = "dat_ref")
@Raw(fileName = "process_a_YYYYMMDD", extension = ".csv", jobName = "proccessA", description = "teste do processo A", database = "databasepro", tableName = "tablepro")
public class ProccessA implements IProccess {

    private final String logStarting = String.format("Starting proccess  %s", ProccessA.class.toString());

    @Override
    public void run(String dt_ref) {

        System.out.println(logStarting);

        System.out.println(dt_ref);

    }
}
