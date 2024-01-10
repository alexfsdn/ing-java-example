package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;
import interfaces.IProcess;
import interfaces.ISpark;
import model.enums.ProcessAEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import utils.TodayUtils;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Partitioned(daily = true, partition = "dat_reference")
@Raw(fileName = "process_a_YYYYMMDD",
        formatDateInTheFileName = "YYYYMMDD",
        extension = "csv",
        jobName = "processA",
        description = "teste do processo A",
        database = "testDB",
        tableName = "tableUser",
        inputHdfs = "src/test/resources/input/",
        outputHdfs = "src/test/resources/output/",
        delimiter = ";",
        header = true
)
public class ProcessA implements IProcess {

    private final String logStarting = String.format("Starting process  %s", ProcessA.class);
    private final String INVALID_LINES = "_corrupt_record";
    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private final String PARTITION_REFERENCE = "PARTITION_REFERENCE";

    @Override
    public void run(ISpark iSpark, String dt_ref) {

        System.out.println(logStarting);

        Class<?> class_ = ProcessA.class;
        Raw raw = class_.getAnnotation(Raw.class);
        Partitioned partitioned = class_.getAnnotation(Partitioned.class);

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
        System.out.println("encoding: " + raw.encoding());
        System.out.println("Pationed: " + partitioned.partition());
        System.out.println("Period daily: " + partitioned.daily());
        System.out.println("Period weekly: " + partitioned.weekly());
        System.out.println("Period mothly: " + partitioned.monthly());
        System.out.println("Period eventual: " + partitioned.eventual());
        System.out.println("---LIST CONFIG THE END---");

        System.out.println("Period: " + dt_ref);

        StructType schema = ProcessAEnum.schema().add(INVALID_LINES, DataTypes.StringType, true);

        String fileName = raw.inputHdfs().concat(raw.fileName().concat(".").concat(raw.extension()).replace(raw.formatDateInTheFileName(), dt_ref));
        String format = raw.extension();

        Map<String, String> map = new HashMap<>();
        map.put("delimiter", raw.delimiter());
        map.put("header", String.valueOf(raw.header()));
        map.put("encoding", raw.encoding());
        map.put("modo", "PERMISSIVE");

        System.out.println("FILE_NAME: " + fileName);
        Dataset<Row> dataset = iSpark.getDataset(fileName, format, map, schema);

        dataset = dataset.filter(col(INVALID_LINES).isNull())
                .drop(col(INVALID_LINES));

        String tableNameTMP = raw.tableName().concat(TodayUtils.getTodayOnlyNumbers());

        dataset = dataset.withColumn(TIME_STAMP_REFERENCE, current_timestamp()).withColumn(PARTITION_REFERENCE, lit(dt_ref))
                .select(col(ProcessAEnum.name.name()),
                        col(ProcessAEnum.age.name()),
                        col(ProcessAEnum.cpf.name()),
                        col(ProcessAEnum.dat_ref.name()),
                        col(TIME_STAMP_REFERENCE),
                        col(PARTITION_REFERENCE));

        dataset.createOrReplaceTempView(tableNameTMP);

        String tableName = raw.database().concat(".").concat(raw.tableName());


        List<String> columns = Arrays.asList(
                ProcessAEnum.name.name(),
                ProcessAEnum.age.name(),
                ProcessAEnum.cpf.name(),
                ProcessAEnum.dat_ref.name(),
                TIME_STAMP_REFERENCE);

        iSpark.save(columns, tableName, tableNameTMP, partitioned.partition(), dt_ref);

        System.out.println("process success");

    }
}
