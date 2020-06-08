package Benchmark.Ingestion;

import Benchmark.*;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;

/**
 * During ingestion, this class monitors and controls the ingest-process.
 */
public class IngestTarget implements ITarget {
    private final CoarseTimer totalTimer;
    private final CoarseTimer reportTimer;
    private final CoarseTimer speedTimer;
    private final PreciseTimer busyTimer;
    private final int desiredIngestSpeedPer100Millis;
    private long totalCounter;
    private int reportCounter;
    private int speedCounter;
    private boolean timersStarted;
    private final boolean shouldThrottleIngestion;
    private final boolean reportIntermediateStats;
    private final int reportFrequencyMillis;
    private final DateCommunication dateComm;
    private final String threadName;
    private final boolean doDirectComm;
    private long sleepDuration = 0;

    private final boolean doCsvLogging;
    private final CSVLogger.IngestLogger csvLogger;
    private volatile boolean stop;

    public IngestTarget(int desiredIngestSpeed, int reportFrequency, DateCommunication dateComm, String threadName,
                        boolean doDirectComm, boolean doCsvLogging, CSVLogger.IngestLogger csvLogger){
        this.desiredIngestSpeedPer100Millis = desiredIngestSpeed / 10;
        this.shouldThrottleIngestion = desiredIngestSpeed > 0;
        this.reportFrequencyMillis = reportFrequency * 1000;
        this.reportIntermediateStats = reportFrequency > 0;
        this.dateComm = dateComm;
        this.threadName = threadName;
        this.doDirectComm = doDirectComm;
        this.doCsvLogging = doCsvLogging;
        this.csvLogger = csvLogger;
        totalTimer = new CoarseTimer();
        reportTimer = new CoarseTimer();
        speedTimer = new CoarseTimer();
        busyTimer = new PreciseTimer();
    }

    public void printFinalStats(){
        double time = totalTimer.elapsedMilliseconds();
        double timeSec = time / 1000;

        String message = String.format("%d entries were added in %.2f seconds. Avg. speed was %.0f / sec.",
                totalCounter, timeSec, totalCounter / timeSec);

        Logger.LOG(String.format("%s: DONE: %s", threadName, message));
        if(doCsvLogging) CSVLogger.GeneralLogger.createOrGetInstance().write(threadName, message);
    }

    public void add(IGeneratedEntry entry) {
        totalCounter++;

        // Update the info about what the newest entry is, so we can use it in queries.
        // However, updating that info requires taking a lock, so rarely do that update.
        if(doDirectComm && totalCounter % 20000 == 0){
            dateComm.setNewestTime(entry.getDateTime());
        }

        if(!timersStarted) {
            totalTimer.start();
            reportTimer.start();
            speedTimer.start();
            timersStarted = true;
        }

        if(reportIntermediateStats){
            reportCounter++;
            double elapsedMillis = reportTimer.elapsedMilliseconds();
            if(elapsedMillis > reportFrequencyMillis){
                int averageOverTime = reportCounter / (reportFrequencyMillis/1000);
                Logger.LOG(String.format("%s: RUNNING %d entries / sec.", threadName, averageOverTime));
                if(doCsvLogging) csvLogger.write(reportCounter, averageOverTime);

                reportCounter = 0;
                reportTimer.start();
            }
        }

        if(shouldThrottleIngestion){
            // Since the 'add' method is a part of the tight ingestion-loop, we can spread our throttle-delays out over
            //   multiple insertions so that we do e.g. "1 insert, wait 2ms, 1 insert, wait 2ms, etc" to insert
            //   ~500 rows in 1 second.
            //   The alternative would be to simply start a timer, insert 500 rows as fast as possible and then
            //   sleeping for the remaining time.
            // This delay-estimation isn't the best and will take some time to stabilize, especially on big batch-sizes,
            //   but has been tested at batch-sizes varying from 100-100000 with desired throttling speeds of 10-40000.
            speedCounter++;
            double elapsedMillis = speedTimer.elapsedMilliseconds();
            // Recalculate our delay every 100 milliseconds (so we can calculate how much we ended up insertion over the last 100 and revise our delay-estimate.
            if(elapsedMillis > 100){
                if(speedCounter > desiredIngestSpeedPer100Millis){
                    sleepDuration += (speedCounter - desiredIngestSpeedPer100Millis)*2 + 5000;
                } else {
                    sleepDuration -= 500;
                }

                speedCounter = 0;
                speedTimer.start();
            }

            if(sleepDuration <= 0) {
                sleepDuration = 0;
            } else {
                busyTimer.start();
                while(busyTimer.elapsedNanoseconds() < sleepDuration){
                    // Busy loop
                }
            }
        }
    }

    public void setStop(){
        stop = true;
    }

    @Override
    public boolean shouldStopEarly() {
        return stop;
    }

    @Override
    public void close() {
        // Nothing to clean-up
    }
}
