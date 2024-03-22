package test.util;

import java.io.*;
import java.util.*;

/** Test series report.

 <p>
 This class contains three lists: a list of tests that succeeded, a list of
 tests that were not run, and a list of reports from tests that failed. Tests
 not run are so because prerequisite tests did not complete successfully, or
 were not run at all.
 */
public class SeriesReport implements Serializable {
    /** Successful tests. */
    private final List<Class<? extends Test>> successful_tests;
    /** Tests that were not run due to missing prerequisites. */
    private final List<Class<? extends Test>> abandoned_tests;
    /** Reports from tests that failed. */
    private final List<TestReport> failure_reports;

    /** Creates a <code>SeriesReport</code>. */
    SeriesReport(List<Class<? extends Test>> successful_tests,
                 List<Class<? extends Test>> abandoned_tests,
                 List<TestReport> failure_reports) {
        this.successful_tests = successful_tests;
        this.abandoned_tests = abandoned_tests;
        this.failure_reports = failure_reports;
    }

    /** Returns <code>true</code> if and only if the test series completed
     successfully.

     <p>
     The test series is considered to have completed successfully if all
     tests have been run, and no tests failed.
     */
    public boolean successful() {
        return (abandoned_tests.size() == 0) && (failure_reports.size() == 0);
    }

    /** Prints a summary that includes the number of tests passed, failed, and
     not run.

     @param stream Stream to receive formatted output.
     */
    public void printSummary(PrintStream stream, Map<String, Integer> points, int reps) {
        stream.println("passed: " + successful_tests.size() + " ---- " +
                "failed: " + failure_reports.size() + " ---- " +
                "not run: " + abandoned_tests.size());
        
        double total = 0.0, p=0.85;
        
        for(Integer i : points.values()) {
            total += Double.valueOf(i);
        }
        if(total == 0.0)
            stream.println("score: 0.00 / 0");
        else if(successful_tests.size() > 0) {
            double score = 0.0;
            Iterator<Class<? extends Test>> iter = successful_tests.iterator();
            
            String pName = iter.next().getCanonicalName(), cName;
            int pcnt = 1;
            while(iter.hasNext()) {
                cName = iter.next().getCanonicalName();
                if(cName.equals(pName)) {
                    pcnt++;
                } else {
                    score += (Math.pow(p,reps - pcnt) - Math.pow(p,reps)) / (1 - Math.pow(p,reps)) 
                              * Double.valueOf(points.get(pName));
                    pName = cName;
                    pcnt = 1;
                }
            }
            score += (Math.pow(p,reps - pcnt) - Math.pow(p,reps)) / (1 - Math.pow(p,reps)) 
                      * Double.valueOf(points.get(pName));
            
            stream.println("score: " + String.format("%.2f", score) + " / " + total);            
        }
    }

    /** Prints the test series report.

     <p>
     The report includes individual test reports for each failed test, a list
     of abandoned tests (if any), and a summary.

     @param stream Stream to receive formatted output.
     */
    public void print(PrintStream stream, Map<String, Integer> points, int reps) {
        // Print the failed test reports.
        for(TestReport failed_test : failure_reports) {
            failed_test.print(stream);
            stream.println();
        }

        // If there are tests that were not run, list them.
        if(abandoned_tests.size() > 0) {
            stream.print("tests not run (up to " + reps + "x each) because of missing prerequisites: ");

            Iterator<Class<? extends Test>> test_iterator =
                    abandoned_tests.iterator();

            String pName = test_iterator.next().getCanonicalName(), cName;
            stream.print(pName);

            while(test_iterator.hasNext()) {
                cName = test_iterator.next().getCanonicalName();
                if(!cName.equals(pName)) {
                    stream.print(", " + cName);
                    pName = cName;
                }
            }

            stream.println();
            stream.println();
        }

        // Print the test series summary.
        printSummary(stream, points, reps);
    }
}
