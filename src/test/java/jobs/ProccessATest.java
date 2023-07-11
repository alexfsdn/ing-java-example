package jobs;

import org.junit.Test;

public class ProccessATest {


    @Test
    public void testReproccess() {
        String args[] = {"ProccessA", "20220812"};
        JobRun.main(args);
    }

    @Test
    public void test() {
        String args[] = {"ProccessA"};
        JobRun.main(args);
    }

    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToJobName() {
        String args[] = {"ProccessNotExist"};
        JobRun.main(args);
    }

    @Test(expected = IllegalArgumentException.class)
    public void IllegalArgumentExcepetionTestToDateInvalid() {
        String args[] = {"ProccessA", "123CantLettler"};
        JobRun.main(args);
    }
}
