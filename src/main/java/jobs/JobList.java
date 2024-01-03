package jobs;

import interfaces.IProcess;
import jobs.raw.ProcessA;
import jobs.raw.LivroProcess;

import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

public class JobList {

    public IProcess job(String className) throws URISyntaxException {
        Map<String, IProcess> jobList = new LinkedHashMap<>();

        jobList.put("ProcessA", new ProcessA());
        jobList.put("ProcessB", new LivroProcess());

        if (jobList.containsKey(className)) {
           return jobList.get(className);

        }

        throw new IllegalArgumentException(String.format("Job %s not found", className));
    }
}
