package annotations;

import jobs.raw.ProcessA;
import org.junit.Test;

public class RawTest {

    @Test
    public void test() {
        Class<?> class_ = ProcessA.class;
        Raw raw = class_.getAnnotation(Raw.class);

        System.out.println("---SHOW---");
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
        System.out.println("---THE END---");
    }

}
