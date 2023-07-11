package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;

@Partitioned(daily = true, partition = "dat_ref")
@Raw(fileName = "process_a_YYYYMMDD", extension = ".csv", jobName = "proccessA", description = "teste do processo A", database = "databasepro", tableName = "tablepro")
public class ProccessA {

}
