package jobs;

import interfaces.IProccess;
import jobs.raw.ProccessA;
import jobs.raw.LivroProccess;

import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

public class JobList {

    public IProccess job(String className) throws URISyntaxException {
        Map<String, IProccess> jobList = new LinkedHashMap<>();

        jobList.put("ProccessA", new ProccessA());
        jobList.put("ProccessB", new LivroProccess());

        if (jobList.containsKey(className)) {
           return jobList.get(className);

        }

        throw new IllegalArgumentException(String.format("Job %s not found", className));
    }
}
