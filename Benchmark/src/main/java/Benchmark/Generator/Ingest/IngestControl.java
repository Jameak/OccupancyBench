package Benchmark.Generator.Ingest;

import Benchmark.CoarseTimer;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.GeneratedEntry;
import Benchmark.Logger;
import Benchmark.PreciseTimer;

/**
 * During ingestion, this class monitors and controls the ingest-process.
 */
public class IngestControl {
    private final CoarseTimer totalTimer;
    private final CoarseTimer reportTimer;
    private final CoarseTimer speedTimer;
    private final PreciseTimer busyTimer;
    private final int desiredIngestSpeedPer100Millis;
    private long totalCounter;
    private int reportCounter;
    private int speedCounter;
    private boolean timersStarted;
    private boolean limitSpeed;
    private final boolean reportIntermediateStats;
    private final int reportFrequencyMillis;
    private final DateCommunication dateComm;
    private final String threadName;
    private final boolean doDirectComm;
    private long sleepDuration = 0;

    public IngestControl(int desiredIngestSpeed, int reportFrequency, DateCommunication dateComm, String threadName, boolean doDirectComm){
        this.desiredIngestSpeedPer100Millis = desiredIngestSpeed / 10;
        this.limitSpeed = desiredIngestSpeed > 0;
        this.reportFrequencyMillis = reportFrequency * 1000;
        this.reportIntermediateStats = reportFrequency > 0;
        this.dateComm = dateComm;
        this.threadName = threadName;
        this.doDirectComm = doDirectComm;
        totalTimer = new CoarseTimer();
        reportTimer = new CoarseTimer();
        speedTimer = new CoarseTimer();
        busyTimer = new PreciseTimer();
    }

    public void printFinalStats(){
        Logger.LOG(String.format("DONE %s: %d entries were added in %.2f seconds.", threadName, totalCounter, totalTimer.elapsedMilliseconds() / 1000));
    }

    public void add(GeneratedEntry entry) {
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
                Logger.LOG(String.format("RUNNING %s: %d entries / sec.", threadName, reportCounter / (reportFrequencyMillis/1000)));

                reportCounter = 0;
                reportTimer.start();
            }
        }

        if(limitSpeed){
            speedCounter++;
            double elapsedMillis = speedTimer.elapsedMilliseconds();
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
}
