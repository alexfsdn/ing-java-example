package jobs;

import jobs.raw.ProccessA;

import java.util.LinkedHashMap;
import java.util.Map;

public class JobList {

    public IProccess job(String className) {
        Map<String, IProccess> jobList = new LinkedHashMap<>();

        jobList.put("ProccessA", new ProccessA());

        if (jobList.containsKey(className)) {
           return jobList.get(className);

        }

        throw new IllegalArgumentException(String.format("Job %s not found", className));
    }
}
