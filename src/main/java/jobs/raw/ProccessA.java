package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;
import interfaces.IProccess;

@Partitioned(daily = true, partition = "dat_ref")
@Raw(fileName = "process_a_YYYYMMDD",
        extension = ".csv",
        jobName = "proccessA",
        description = "teste do processo A",
        database = "databasepro",
        tableName = "tablepro",
        inputHdfs = "/data/input/",
        outputHdfs = "/data/output/",
        delimiter = ";",
        header = true
)
public class ProccessA implements IProccess {

    private final String logStarting = String.format("Starting proccess  %s", ProccessA.class);

    @Override
    public void run(String dt_ref) {

        System.out.println(logStarting);

        Class<?> class_ = ProccessA.class;
        Raw raw = class_.getAnnotation(Raw.class);

        System.out.println("---LIST CONFIG---");
        System.out.println("FileName: " + raw.fileName());
        System.out.println("Extesion: " + raw.extension());
        System.out.println("JobName: " + raw.jobName());
        System.out.println("Description: " + raw.description());
        System.out.println("Database: " + raw.database());
        System.out.println("TableName: " + raw.tableName());
        System.out.println("Delimiter: " + raw.delimiter());
        System.out.println("Input hdfs: " + raw.inputHdfs());
        System.out.println("Output hdfs: " + raw.outputHdfs());
        System.out.println("Layer: " + raw.layer());
        System.out.println("Header: " + raw.header());
        System.out.println("---LIST CONFIG THE END---");

        System.out.println("Period: " + dt_ref);


    }
}
