package Benchmark.Queries;

import Benchmark.CoarseTimer;
import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Logger;
import Benchmark.PreciseTimer;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The top-level class for benchmark queries. Monitors query-performance, decides which queries to run
 * and generates their arguments.
 */
public class QueryRunnable implements Runnable {
    private final ConfigFile config;
    private final Random rng;
    private final DateCommunication dateComm;
    private final Floor[] generatedFloors;
    private final AccessPoint[] allAPs;
    private final Queries queryTarget;
    private final String threadName;
    private final int minTimeInterval;
    private final int maxTimeInterval;

    private final PreciseTimer timerQuery_TotalClients;
    private final PreciseTimer timerQuery_FloorTotal;
    private final PreciseTimer timerQuery_MaxForAP;

    private final int totalThreshold;
    private final int thresholdQuery_TotalClients;
    private final int thresholdQuery_FloorTotal;
    private final int thresholdQuery_MaxForAP;

    private int countFull;
    private int countQueryInProg_TotalClients;
    private int countQueryInProg_FloorTotal;
    private int countQueryInProg_MaxForAP;
    private int countQueryDone_TotalClients;
    private int countQueryDone_FloorTotal;
    private int countQueryDone_MaxForAP;

    private long timeSpentQueryInProg_TotalClients;
    private long timeSpentQueryInProg_FloorTotal;
    private long timeSpentQueryInProg_MaxForAP;
    private long timeSpentQueryDone_TotalClients;
    private long timeSpentQueryDone_FloorTotal;
    private long timeSpentQueryDone_MaxForAP;

    public QueryRunnable(ConfigFile config, Random rng, DateCommunication dateComm, Floor[] generatedFloors, Queries queryTarget, String threadName){
        this.config = config;
        this.rng = rng;
        this.dateComm = dateComm;
        this.generatedFloors = generatedFloors;
        this.queryTarget = queryTarget;
        this.threadName = threadName;
        this.timerQuery_TotalClients = new PreciseTimer();
        this.timerQuery_FloorTotal = new PreciseTimer();
        this.timerQuery_MaxForAP = new PreciseTimer();

        assert this.config.getQueriesWeightTotalClients() >= 0;
        this.thresholdQuery_TotalClients = config.getQueriesWeightTotalClients();
        assert this.config.getQueriesWeightFloorTotals() >= 0;
        this.thresholdQuery_FloorTotal = thresholdQuery_TotalClients + config.getQueriesWeightFloorTotals();
        assert this.config.getQueriesWeightMaxForAP() >= 0;
        this.thresholdQuery_MaxForAP = thresholdQuery_FloorTotal + config.getQueriesWeightMaxForAP();

        this.totalThreshold = config.getQueriesWeightTotalClients() + config.getQueriesWeightFloorTotals() + config.getQueriesWeightMaxForAP();

        this.minTimeInterval = config.getQueriesIntervalMin();
        this.maxTimeInterval = config.getQueriesIntervalMax();

        List<AccessPoint> APs = new ArrayList<>();
        for(Floor floor : generatedFloors){
            APs.addAll(Arrays.asList(floor.getAPs()));
        }
        allAPs = APs.toArray(new AccessPoint[0]);
    }

    private QueryType selectQuery(){
        int val = rng.nextInt(totalThreshold);
        if(val < thresholdQuery_TotalClients) return QueryType.TotalClients;
        else if (val < thresholdQuery_FloorTotal) return QueryType.FloorTotals;
        else if (val < thresholdQuery_MaxForAP) return QueryType.MaxForAP;
        return QueryType.UNKNOWN;
    }

    private void runQueries(Queries queries, boolean warmUp) throws SQLException{
        switch (selectQuery()){
            case TotalClients:
            {
                LocalDateTime[] time = generateTime();
                if(!warmUp) timerQuery_TotalClients.start();
                queries.computeTotalClients(time[0], time[1]);
                if(!warmUp) {
                    timeSpentQueryInProg_TotalClients += timerQuery_TotalClients.elapsedNanoseconds();
                    countQueryInProg_TotalClients++;
                }
            }
                break;
            case FloorTotals:
            {
                LocalDateTime[] time = generateTime();
                if(!warmUp) timerQuery_FloorTotal.start();
                queries.computeFloorTotal(time[0], time[1], generatedFloors);
                if(!warmUp){
                    timeSpentQueryInProg_FloorTotal += timerQuery_FloorTotal.elapsedNanoseconds();
                    countQueryInProg_FloorTotal++;
                }
            }
                break;
            case MaxForAP:
            {
                LocalDateTime[] time = generateTime();
                if(!warmUp) timerQuery_MaxForAP.start();
                AccessPoint selectedAP = selectRandomAP();
                queries.maxPerDayForAP(time[0], time[1], selectedAP);
                if(!warmUp){
                    timeSpentQueryInProg_MaxForAP += timerQuery_MaxForAP.elapsedNanoseconds();
                    countQueryInProg_MaxForAP++;
                }
            }
                break;
            default:
            case UNKNOWN:
                assert false;
                Logger.LOG(threadName + ": WTF. Invalid query probabilities");
                throw new RuntimeException();
        }
        if(!warmUp) countFull++;
    }

    private AccessPoint selectRandomAP() {
        return allAPs[rng.nextInt(allAPs.length)];
    }

    private LocalDateTime[] generateTime(){
        LocalDate earliestValidDate = config.getQueriesEarliestValidDate();
        LocalDateTime startClamp = LocalDateTime.of(earliestValidDate, LocalTime.of(0,0,0));
        LocalDateTime[] time = new LocalDateTime[2];
        LocalDateTime newestValidDate = dateComm.getNewestTime();

        LocalDateTime time1 = generateRandomTime(newestValidDate, startClamp);
        LocalDateTime time2 = generateRandomTime(newestValidDate, startClamp);
        if(time1.isBefore(time2)){
            time[0] = time1;
            time[1] = time2;
        } else {
            time[1] = time1;
            time[0] = time2;
        }

        if(time[0].isBefore(startClamp))     time[0] = startClamp;
        if(time[1].isAfter(newestValidDate)) time[1] = newestValidDate;

        // Make sure that the interval obeys the minimum size. If the interval is smaller than minimum size, then we bias towards the newest values.
        if(time[0].plusSeconds(minTimeInterval).isAfter(time[1])){
            time[1] = time[0].plusSeconds(minTimeInterval);
            if(time[1].isAfter(newestValidDate)) {
                time[1] = newestValidDate;
                time[0] = newestValidDate.minusSeconds(minTimeInterval);
                if(time[0].isBefore(startClamp)) time[0] = startClamp;
            }
        }

        // Make sure that the interval obeys the maximum size. If the interval is larger than the maximum size, then we bias towards the oldest values.
        if(time[0].plusSeconds(maxTimeInterval).isBefore(time[1])){
            time[1] = time[0].plusSeconds(maxTimeInterval);
        }

        assert time[0].isBefore(time[1]) || time[0].isEqual(time[1]);
        assert !time[0].isBefore(startClamp);
        assert !time[1].isAfter(newestValidDate);
        return time;
    }

    private LocalDateTime generateRandomTime(LocalDateTime newestValue, LocalDateTime earliestValue){
        double choice = rng.nextDouble();
        if(choice < config.getQueriesRngRangeDay()){
            // Query for some time on the newest date.
            return randomTimeBetween(newestValue.minusDays(1), newestValue, earliestValue);
        } else if(choice < config.getQueriesRngRangeWeek()){
            // Query for some time within the last 7 days.
            return randomTimeBetween(newestValue.minusDays(6), newestValue, earliestValue);
        } else if(choice < config.getQueriesRngRangeMonth()){
            // Query: within the last 30 days
            return randomTimeBetween(newestValue.minusDays(29), newestValue, earliestValue);
        } else if(choice < config.getQueriesRngRangeYear()){
            // Query: within the last 365 days
            return randomTimeBetween(newestValue.minusDays(364), newestValue, earliestValue);
        } else {
            // Query: any value within the valid interval
            return randomTimeBetween(earliestValue, newestValue, earliestValue);
        }
    }

    private LocalDateTime randomTimeBetween(LocalDateTime start, LocalDateTime end, LocalDateTime clamp){
        if(start.isBefore(clamp)) start = clamp;
        assert start.isBefore(end);
        assert clamp.isBefore(start) || clamp.isEqual(start);
        long startSeconds = start.toEpochSecond(ZoneOffset.ofHours(0));
        int diffSeconds = Math.toIntExact(end.toEpochSecond(ZoneOffset.ofHours(0)) - startSeconds);
        assert diffSeconds > 0;
        long randBetween = startSeconds + rng.nextInt(diffSeconds);
        return LocalDateTime.ofEpochSecond(randBetween, 0, ZoneOffset.ofHours(0));
    }

    private void reportStats(boolean done){
        String prefix = (done ? "DONE " : "RUNNING ") + threadName;
        int count_TotalClients      = done ? countQueryDone_TotalClients     : countQueryInProg_TotalClients;
        int count_FloorTotal        = done ? countQueryDone_FloorTotal       : countQueryInProg_FloorTotal;
        int count_MaxForAP          = done ? countQueryDone_MaxForAP         : countQueryInProg_MaxForAP;
        long timeSpent_TotalClients = done ? timeSpentQueryDone_TotalClients : timeSpentQueryInProg_TotalClients;
        long timeSpent_FloorTotal   = done ? timeSpentQueryDone_FloorTotal   : timeSpentQueryInProg_FloorTotal;
        long timeSpent_MaxForAP     = done ? timeSpentQueryDone_MaxForAP     : timeSpentQueryInProg_MaxForAP;

        if(!done) Logger.LOG(String.format("%s: Average query-speeds from the last %s seconds:", prefix, config.getQueriesReportingFrequency()));
        Logger.LOG(String.format("%s: Query name      |    Count |   Total time |  Queries / sec", prefix));
        Logger.LOG(String.format("%s: 'Total Clients' | %8d | %8.1f sec | %8.1f / sec", prefix, count_TotalClients, timeSpent_TotalClients / 1e9, count_TotalClients / (timeSpent_TotalClients / 1e9) ));
        Logger.LOG(String.format("%s: 'Floor Totals'  | %8d | %8.1f sec | %8.1f / sec", prefix, count_FloorTotal, timeSpent_FloorTotal / 1e9, count_FloorTotal / (timeSpent_FloorTotal / 1e9) ));
        Logger.LOG(String.format("%s: 'Max for AP'    | %8d | %8.1f sec | %8.1f / sec", prefix, count_MaxForAP, timeSpent_MaxForAP / 1e9, count_MaxForAP / (timeSpent_MaxForAP / 1e9) ));
        Logger.LOG(String.format("%s: ----------------|----------|--------------|---------------", prefix));

        if(!done){
            countQueryDone_TotalClients += countQueryInProg_TotalClients;
            countQueryDone_FloorTotal += countQueryInProg_FloorTotal;
            countQueryDone_MaxForAP += countQueryInProg_MaxForAP;
            timeSpentQueryDone_TotalClients += timeSpentQueryInProg_TotalClients;
            timeSpentQueryDone_FloorTotal += timeSpentQueryInProg_FloorTotal;
            timeSpentQueryDone_MaxForAP += timeSpentQueryInProg_MaxForAP;
            countQueryInProg_TotalClients = 0;
            countQueryInProg_FloorTotal = 0;
            countQueryInProg_MaxForAP = 0;
            timeSpentQueryInProg_TotalClients = 0;
            timeSpentQueryInProg_FloorTotal = 0;
            timeSpentQueryInProg_MaxForAP = 0;
        }

        if(done){
            double totalTime = (timeSpentQueryDone_TotalClients + timeSpentQueryDone_FloorTotal + timeSpentQueryDone_MaxForAP) / 1e9;
            Logger.LOG(String.format("%s: TOTAL           | %8d | %8.1f sec | %8.1f / sec", prefix, countFull, totalTime, countFull / totalTime ));
        }
    }

    @Override
    public void run() {
        int duration = config.getQueriesDuration();
        int warmUpTime = config.getQueriesWarmupDuration();
        int reportFrequency = config.getQueriesReportingFrequency();
        int targetCount = config.getMaxQueryCount();
        CoarseTimer runTimer = new CoarseTimer();
        CoarseTimer reportTimer = new CoarseTimer();
        boolean warmUp = warmUpTime > 0;
        boolean printProgressReports =  reportFrequency > 0;

        try {
            queryTarget.prepare(config, generatedFloors);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (warmUp) {
            try {
                runTimer.start();
                while (runTimer.elapsedSeconds() < warmUpTime) {
                    runQueries(queryTarget, true);
                }
            } catch (SQLException e) {
                Logger.LOG(threadName + ": SQL Exception during querying warmup.");
                e.printStackTrace();
            }
        }

        try {
            if(duration > 0){
                runTimer.start();
                if(printProgressReports) reportTimer.start();
                while (runTimer.elapsedSeconds() < duration) {
                    runQueries(queryTarget, false);

                    if(printProgressReports) progressReport(reportTimer, reportFrequency);
                }
            } else {
                assert targetCount > 0;
                if(printProgressReports) reportTimer.start();
                while (countFull < targetCount) {
                        runQueries(queryTarget, false);

                    if(printProgressReports) progressReport(reportTimer, reportFrequency);
                }
            }
        } catch (SQLException e) {
            Logger.LOG(threadName + ": SQL Exception during querying.");
            e.printStackTrace();
        }

        try {
            queryTarget.done();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Logger.LOG(() -> reportStats(true));
    }

    private void progressReport(CoarseTimer timer, int frequency){
        if(timer.elapsedSeconds() > frequency){
            Logger.LOG(() -> reportStats(false));
            timer.start();
        }
    }

    private enum QueryType{
        TotalClients, FloorTotals, MaxForAP, UNKNOWN
    }
}
