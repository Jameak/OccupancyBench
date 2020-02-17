package Benchmark.Queries;

import Benchmark.CoarseTimer;
import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Logger;
import Benchmark.PreciseTimer;
import Benchmark.Queries.Results.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
    private final Random rngQueries;
    private final DateCommunication dateComm;
    private final Floor[] generatedFloors;
    private final AccessPoint[] allAPs;
    private final IQueries queryTarget;
    private final String threadName;
    private final int threadNumber;
    private final int generalMinTimeInterval;
    private final int generalMaxTimeInterval;

    private final boolean saveQueryResults;
    private List<QueryResult> queryResults = new ArrayList<>();
    private int queryId = 0;

    private final PreciseTimer timerQuery_TotalClients;
    private final PreciseTimer timerQuery_FloorTotal;
    private final PreciseTimer timerQuery_MaxForAP;
    private final PreciseTimer timerQuery_AvgOccupancy;
    private final PreciseTimer timerQuery_KMeans;

    private final int totalThreshold;
    private final int thresholdQuery_TotalClients;
    private final int thresholdQuery_FloorTotal;
    private final int thresholdQuery_MaxForAP;
    private final int thresholdQuery_AvgOccupancy;
    private final int thresholdQuery_KMeans;

    private int countFull;
    private int countQueryInProg_TotalClients;
    private int countQueryInProg_FloorTotal;
    private int countQueryInProg_MaxForAP;
    private int countQueryInProg_AvgOccupancy;
    private int countQueryInProg_KMeans;
    private int countQueryDone_TotalClients;
    private int countQueryDone_FloorTotal;
    private int countQueryDone_MaxForAP;
    private int countQueryDone_AvgOccupancy;
    private int countQueryDone_KMeans;

    private long timeSpentQueryInProg_TotalClients;
    private long timeSpentQueryInProg_FloorTotal;
    private long timeSpentQueryInProg_MaxForAP;
    private long timeSpentQueryInProg_AvgOccupancy;
    private long timeSpentQueryInProg_KMeans;
    private long timeSpentQueryDone_TotalClients;
    private long timeSpentQueryDone_FloorTotal;
    private long timeSpentQueryDone_MaxForAP;
    private long timeSpentQueryDone_AvgOccupancy;
    private long timeSpentQueryDone_KMeans;

    private LocalDateTime newestValidDate;
    private LocalDateTime unmodifiedNewestValidDate;

    public QueryRunnable(ConfigFile config, Random rng, DateCommunication dateComm, Floor[] generatedFloors, IQueries queryTarget, String threadName, int threadNumber){
        this.config = config;
        this.rngQueries = rng;
        this.dateComm = dateComm;
        this.generatedFloors = generatedFloors;
        this.queryTarget = queryTarget;
        this.threadName = threadName;
        this.threadNumber = threadNumber;
        this.timerQuery_TotalClients = new PreciseTimer();
        this.timerQuery_FloorTotal = new PreciseTimer();
        this.timerQuery_MaxForAP = new PreciseTimer();
        this.timerQuery_AvgOccupancy = new PreciseTimer();
        this.timerQuery_KMeans = new PreciseTimer();
        this.newestValidDate = dateComm.getNewestTime();
        this.unmodifiedNewestValidDate = newestValidDate;
        this.saveQueryResults = config.DEBUG_saveQueryResults();

        assert this.config.getQueriesWeightTotalClients() >= 0;
        assert this.config.getQueriesWeightFloorTotals() >= 0;
        assert this.config.getQueriesWeightMaxForAP() >= 0;
        assert this.config.getQueriesWeightAvgOccupancy() >= 0;
        assert this.config.getQueriesWeightKMeans() >= 0;
        this.thresholdQuery_TotalClients = config.getQueriesWeightTotalClients();
        this.thresholdQuery_FloorTotal = thresholdQuery_TotalClients + config.getQueriesWeightFloorTotals();
        this.thresholdQuery_MaxForAP = thresholdQuery_FloorTotal + config.getQueriesWeightMaxForAP();
        this.thresholdQuery_AvgOccupancy = thresholdQuery_MaxForAP + config.getQueriesWeightAvgOccupancy();
        this.thresholdQuery_KMeans = thresholdQuery_AvgOccupancy + config.getQueriesWeightKMeans();
        this.totalThreshold =
                config.getQueriesWeightTotalClients() +
                config.getQueriesWeightFloorTotals() +
                config.getQueriesWeightMaxForAP() +
                config.getQueriesWeightAvgOccupancy() +
                config.getQueriesWeightKMeans();

        this.generalMinTimeInterval = config.getQueriesIntervalMin();
        this.generalMaxTimeInterval = config.getQueriesIntervalMax();

        List<AccessPoint> APs = new ArrayList<>();
        for(Floor floor : generatedFloors){
            APs.addAll(Arrays.asList(floor.getAPs()));
        }
        allAPs = APs.toArray(new AccessPoint[0]);
    }

    private QueryType selectQuery(Random rng){
        int val = rng.nextInt(totalThreshold);
        if(val < thresholdQuery_TotalClients) return QueryType.TotalClients;
        else if (val < thresholdQuery_FloorTotal) return QueryType.FloorTotals;
        else if (val < thresholdQuery_MaxForAP) return QueryType.MaxForAP;
        else if (val < thresholdQuery_AvgOccupancy) return QueryType.AvgOccupancy;
        else if (val < thresholdQuery_KMeans) return QueryType.KMeans;
        return QueryType.UNKNOWN;
    }

    private void runQueries(IQueries queries, boolean warmUp, Random rng) throws IOException, SQLException{
        switch (selectQuery(rng)){
            case TotalClients:
            {
                LocalDateTime[] time = generateTimeInterval(rng, generalMinTimeInterval, generalMaxTimeInterval);
                if(!warmUp) timerQuery_TotalClients.start();

                List<Total> result = queries.computeTotalClients(time[0], time[1]);

                if(!warmUp) {
                    timeSpentQueryInProg_TotalClients += timerQuery_TotalClients.elapsedNanoseconds();
                    countQueryInProg_TotalClients++;
                }

                if(saveQueryResults && !warmUp) {
                    queryResults.add(new QueryResult(queryId, QueryType.TotalClients, result));
                }
            }
                break;
            case FloorTotals:
            {
                LocalDateTime[] time = generateTimeInterval(rng, generalMinTimeInterval, generalMaxTimeInterval);
                if(!warmUp) timerQuery_FloorTotal.start();

                List<FloorTotal> result = queries.computeFloorTotal(time[0], time[1]);

                if(!warmUp){
                    timeSpentQueryInProg_FloorTotal += timerQuery_FloorTotal.elapsedNanoseconds();
                    countQueryInProg_FloorTotal++;
                }

                if(saveQueryResults && !warmUp) {
                    queryResults.add(new QueryResult(queryId, QueryType.FloorTotals, result));
                }
            }
                break;
            case MaxForAP:
            {
                LocalDateTime[] time = generateTimeInterval(rng, generalMinTimeInterval, generalMaxTimeInterval);
                AccessPoint selectedAP = selectRandomAP(rng);
                if(!warmUp) timerQuery_MaxForAP.start();

                List<MaxForAP> result = queries.maxPerDayForAP(time[0], time[1], selectedAP);

                if(!warmUp){
                    timeSpentQueryInProg_MaxForAP += timerQuery_MaxForAP.elapsedNanoseconds();
                    countQueryInProg_MaxForAP++;
                }

                if(saveQueryResults && !warmUp) {
                    queryResults.add(new QueryResult(queryId, QueryType.MaxForAP, result));
                }
            }
                break;
            case AvgOccupancy:
            {
                LocalDateTime startTime = generateTime(newestValidDate, rng);
                if(!warmUp) timerQuery_AvgOccupancy.start();

                List<AvgOccupancy> result = queries.computeAvgOccupancy(startTime, newestValidDate, 5);

                if(!warmUp){
                    timeSpentQueryInProg_AvgOccupancy += timerQuery_AvgOccupancy.elapsedNanoseconds();
                    countQueryInProg_AvgOccupancy++;
                }

                if(saveQueryResults && !warmUp) {
                    queryResults.add(new QueryResult(queryId, QueryType.AvgOccupancy, result));
                }
            }
                break;
            case KMeans:
            {
                LocalDateTime[] time = generateTimeInterval(rng, config.getQueriesIntervalMinKMeans(), config.getQueriesIntervalMaxKMeans());
                if(!warmUp) timerQuery_KMeans.start();

                List<KMeans> result = queries.computeKMeans(time[0], time[1], config.getQueriesKMeansClusters(), config.getQueriesKMeansIterations());

                if(!warmUp){
                    timeSpentQueryInProg_KMeans += timerQuery_KMeans.elapsedNanoseconds();
                    countQueryInProg_KMeans++;
                }

                if(saveQueryResults && !warmUp) {
                    queryResults.add(new QueryResult(queryId, QueryType.KMeans, result));
                }
            }
                break;
            case UNKNOWN:
            default:
                assert false;
                Logger.LOG(threadName + ": WTF. Invalid query probabilities");
                throw new RuntimeException();
        }
        if(!warmUp) countFull++;
        queryId++;
    }

    private AccessPoint selectRandomAP(Random rng) {
        return allAPs[rng.nextInt(allAPs.length)];
    }

    private LocalDateTime generateTime(LocalDateTime endTime, Random rng){
        LocalDate earliestValidDate = config.getQueriesEarliestValidDate();
        LocalDateTime startClamp = LocalDateTime.of(earliestValidDate, LocalTime.of(0,0,0));
        // If moving the max interval back from the end-time doesn't surpass the earliest valid date, then we can set
        // the earliest valid date to the datetime corresponding to the max interval.
        LocalDateTime farthestBack = endTime.minusSeconds(generalMaxTimeInterval);
        if(farthestBack.isBefore(startClamp)){
            farthestBack = startClamp;
        }

        LocalDateTime startTime = generateRandomTime(newestValidDate, farthestBack, rng);
        assert startTime.isBefore(endTime);

        // Make sure that the interval obeys the min interval size. We know it already obeys the max interval size so
        //   no need to check that.
        if(startTime.plusSeconds(generalMinTimeInterval).isAfter(endTime)){
            startTime = endTime.minusSeconds(generalMinTimeInterval);

            if(startTime.isBefore(startClamp)) startTime = startClamp;
        }

        assert ChronoUnit.SECONDS.between(startTime, endTime) <= generalMaxTimeInterval : "generateTime: Interval between times exceeds the max interval. Start: " + startTime + ", end: " + endTime;
        assert ChronoUnit.SECONDS.between(startTime, endTime) >= generalMinTimeInterval : "generateTime: Interval between times is slower than the min interval. Start: " + startTime + ", end: " + endTime;

        if(config.DEBUG_truncateQueryTimestamps()){
            //Truncating here slightly risks going over/under the min/max interval, but it's by such a small amount
            //  that I dont care since this is a debug-option. This is much simpler code.
            startTime = startTime.truncatedTo(ChronoUnit.MINUTES);
        }

        return startTime;
    }

    private void checkForConfigError() throws IOException, SQLException {
        LocalDate earliestValidDate = config.getQueriesEarliestValidDate();
        LocalDateTime startClamp = LocalDateTime.of(earliestValidDate, LocalTime.of(0,0,0));

        if(startClamp.isAfter(newestValidDate)){
            if(config.isIngestionEnabled()){
                Logger.LOG(threadName + ": Possible configuration error: The configured date for the setting " + ConfigFile.QUERIES_EARLIEST_VALID_DATE +
                        " is after the newest timestamp available in the database. The config-value is " +
                        config.getQueriesEarliestValidDate() + " but the newest timestamp available in the database is "
                        + unmodifiedNewestValidDate + ". Thread will sleep until the newest timestamp available in the database is " +
                        "after the configured date.");
                while(startClamp.isAfter(newestValidDate)){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    getTime(null, true);
                }
                Logger.LOG(threadName + ": Error cleared, continuing.");
            } else {
                Logger.LOG(threadName + ": Possible configuration error: The configured date for the setting " + ConfigFile.QUERIES_EARLIEST_VALID_DATE +
                        " is invalid unless multiple Benchmark-processes are running against the same target. " +
                        "The config-value is " + config.getQueriesEarliestValidDate() + " but the newest timestamp " +
                        "available in the database is " + unmodifiedNewestValidDate + ". Thread will sleep for 10 seconds and then " +
                        "continue on. If the config is actually invalid then expect asserts to be tripped (if enabled).");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private LocalDateTime[] generateTimeInterval(Random rng, int minInterval, int maxInterval) throws IOException, SQLException {
        assert minInterval <= maxInterval;

        LocalDate earliestValidDate = config.getQueriesEarliestValidDate();
        LocalDateTime startClamp = LocalDateTime.of(earliestValidDate, LocalTime.of(0,0,0));

        LocalDateTime[] time = new LocalDateTime[2];

        LocalDateTime time1 = generateRandomTime(newestValidDate, startClamp, rng);
        LocalDateTime time2 = generateRandomTime(newestValidDate, startClamp, rng);
        if(time1.isBefore(time2)){
            time[0] = time1;
            time[1] = time2;
        } else {
            time[1] = time1;
            time[0] = time2;
        }

        if(time[0].isBefore(startClamp))     time[0] = startClamp;
        if(time[1].isAfter(newestValidDate)) time[1] = newestValidDate;

        // Make sure that the interval obeys the minimum size. If the interval is smaller than minimum size,
        // then we bias towards the newest values.
        if(time[0].plusSeconds(minInterval).isAfter(time[1])){
            time[1] = time[0].plusSeconds(minInterval);
            if(time[1].isAfter(newestValidDate)) {
                time[1] = newestValidDate;
                time[0] = newestValidDate.minusSeconds(minInterval);
                if(time[0].isBefore(startClamp)) time[0] = startClamp;
            }
        }

        // Make sure that the interval obeys the maximum size. If the interval is larger than the maximum size,
        // then we bias towards the oldest values.
        if(time[0].plusSeconds(maxInterval).isBefore(time[1])){
            time[1] = time[0].plusSeconds(maxInterval);
        }

        assert time[0].isBefore(time[1]) || time[0].isEqual(time[1]);
        assert !time[0].isBefore(startClamp);
        assert !time[1].isAfter(newestValidDate);
        assert ChronoUnit.SECONDS.between(time[0], time[1]) <= maxInterval : "generateTimeInterval: Interval between times exceeds the max interval. Start: " + time[0] + ", end: " + time[1];
        assert ChronoUnit.SECONDS.between(time[0], time[1]) >= minInterval : "generateTimeInterval: Interval between times is slower than the min interval. Start: " + time[0] + ", end: " + time[1];

        if(config.DEBUG_truncateQueryTimestamps()){
            //Truncating here slightly risks going over/under the min/max interval, but it's by such a small amount
            //  that I dont care since this is a debug-option. This is much simpler code.
            time[0] = time[0].truncatedTo(ChronoUnit.MINUTES);
            time[1] = time[1].truncatedTo(ChronoUnit.MINUTES);
        }

        return time;
    }

    private LocalDateTime generateRandomTime(LocalDateTime newestValue, LocalDateTime earliestValue, Random rng){
        double choice = rng.nextDouble();
        if(choice < config.getQueriesRngRangeDay()){
            // Query for some time on the newest date.
            return randomTimeBetween(newestValue.minusDays(1), newestValue, earliestValue, rng);
        } else if(choice < config.getQueriesRngRangeWeek()){
            // Query for some time within the last 7 days.
            return randomTimeBetween(newestValue.minusDays(6), newestValue, earliestValue, rng);
        } else if(choice < config.getQueriesRngRangeMonth()){
            // Query: within the last 30 days
            return randomTimeBetween(newestValue.minusDays(29), newestValue, earliestValue, rng);
        } else if(choice < config.getQueriesRngRangeYear()){
            // Query: within the last 365 days
            return randomTimeBetween(newestValue.minusDays(364), newestValue, earliestValue, rng);
        } else {
            // Query: any value within the valid interval
            return randomTimeBetween(earliestValue, newestValue, earliestValue, rng);
        }
    }

    private LocalDateTime randomTimeBetween(LocalDateTime start, LocalDateTime end, LocalDateTime clamp, Random rng){
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

        // Update total counters with latest in-progress results.
        countQueryDone_TotalClients += countQueryInProg_TotalClients;
        countQueryDone_FloorTotal +=   countQueryInProg_FloorTotal;
        countQueryDone_MaxForAP +=     countQueryInProg_MaxForAP;
        countQueryDone_AvgOccupancy += countQueryInProg_AvgOccupancy;
        countQueryDone_KMeans +=       countQueryInProg_KMeans;
        timeSpentQueryDone_TotalClients += timeSpentQueryInProg_TotalClients;
        timeSpentQueryDone_FloorTotal +=   timeSpentQueryInProg_FloorTotal;
        timeSpentQueryDone_MaxForAP +=     timeSpentQueryInProg_MaxForAP;
        timeSpentQueryDone_AvgOccupancy += timeSpentQueryInProg_AvgOccupancy;
        timeSpentQueryDone_KMeans +=       timeSpentQueryInProg_KMeans;

        int count_TotalClients      = done ? countQueryDone_TotalClients     : countQueryInProg_TotalClients;
        int count_FloorTotal        = done ? countQueryDone_FloorTotal       : countQueryInProg_FloorTotal;
        int count_MaxForAP          = done ? countQueryDone_MaxForAP         : countQueryInProg_MaxForAP;
        int count_AvgOccupancy      = done ? countQueryDone_AvgOccupancy     : countQueryInProg_AvgOccupancy;
        int count_KMeans            = done ? countQueryDone_KMeans           : countQueryInProg_KMeans;
        long timeSpent_TotalClients = done ? timeSpentQueryDone_TotalClients : timeSpentQueryInProg_TotalClients;
        long timeSpent_FloorTotal   = done ? timeSpentQueryDone_FloorTotal   : timeSpentQueryInProg_FloorTotal;
        long timeSpent_MaxForAP     = done ? timeSpentQueryDone_MaxForAP     : timeSpentQueryInProg_MaxForAP;
        long timeSpent_AvgOccupancy = done ? timeSpentQueryDone_AvgOccupancy : timeSpentQueryInProg_AvgOccupancy;
        long timeSpent_KMeans =       done ? timeSpentQueryDone_KMeans       : timeSpentQueryInProg_KMeans;

        if(!done) Logger.LOG(String.format("%s: Average query-speeds from the last %s seconds:", prefix, config.getQueriesReportingFrequency()));
        Logger.LOG(String.format("%s: Query name      |    Count |   Total time |  Queries / sec", prefix));
        Logger.LOG(String.format("%s: 'Total Clients' | %8d | %8.1f sec | %8.1f / sec", prefix, count_TotalClients, timeSpent_TotalClients / 1e9, count_TotalClients / (timeSpent_TotalClients / 1e9) ));
        Logger.LOG(String.format("%s: 'Floor Totals'  | %8d | %8.1f sec | %8.1f / sec", prefix, count_FloorTotal, timeSpent_FloorTotal / 1e9, count_FloorTotal / (timeSpent_FloorTotal / 1e9) ));
        Logger.LOG(String.format("%s: 'Max for AP'    | %8d | %8.1f sec | %8.1f / sec", prefix, count_MaxForAP, timeSpent_MaxForAP / 1e9, count_MaxForAP / (timeSpent_MaxForAP / 1e9) ));
        Logger.LOG(String.format("%s: 'Avg Occupancy' | %8d | %8.1f sec | %8.1f / sec", prefix, count_AvgOccupancy, timeSpent_AvgOccupancy / 1e9, count_AvgOccupancy / (timeSpent_AvgOccupancy / 1e9) ));
        Logger.LOG(String.format("%s: 'K-Means'       | %8d | %8.1f sec | %8.1f / sec", prefix, count_KMeans, timeSpent_KMeans / 1e9, count_KMeans / (timeSpent_KMeans / 1e9) ));
        Logger.LOG(String.format("%s: ----------------|----------|--------------|---------------", prefix));

        if(done){
            double totalTime = (
                    timeSpentQueryDone_TotalClients +
                    timeSpentQueryDone_FloorTotal +
                    timeSpentQueryDone_MaxForAP +
                    timeSpentQueryDone_AvgOccupancy +
                    timeSpentQueryDone_KMeans
                ) / 1e9;
            Logger.LOG(String.format("%s: TOTAL           | %8d | %8.1f sec | %8.1f / sec", prefix, countFull, totalTime, countFull / totalTime ));
        }

        // Reset in-progress counters
        countQueryInProg_TotalClients = 0;
        countQueryInProg_FloorTotal = 0;
        countQueryInProg_MaxForAP = 0;
        countQueryInProg_AvgOccupancy = 0;
        countQueryInProg_KMeans = 0;
        timeSpentQueryInProg_TotalClients = 0;
        timeSpentQueryInProg_FloorTotal = 0;
        timeSpentQueryInProg_MaxForAP = 0;
        timeSpentQueryInProg_AvgOccupancy = 0;
        timeSpentQueryInProg_KMeans = 0;
    }

    @Override
    public void run() {
        int duration = config.getQueriesDuration();
        int warmUpTime = config.getQueriesWarmupDuration();
        int reportFrequency = config.getQueriesReportingFrequency();
        int targetCount = config.getMaxQueryCount();
        CoarseTimer runTimer = new CoarseTimer();
        CoarseTimer reportTimer = new CoarseTimer();
        PreciseTimer dateCommTimer = new PreciseTimer();
        boolean warmUp = warmUpTime > 0;
        boolean printProgressReports = reportFrequency > 0;
        Random rngWarmup = new Random(rngQueries.nextInt());

        try {
            queryTarget.prepare(config, generatedFloors, new Random(rngQueries.nextInt()));
        } catch (Exception e) {
            Logger.LOG("Exception during 'prepare' call.");
            throw new RuntimeException(e);
        }

        // Validate the config-settings that depend on values in the database and cant be validated statically.
        try {
            getTime(null, true);
            checkForConfigError();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        if (warmUp) {
            Logger.LOG(threadName + ": Query warm-up has started.");
            try {
                dateCommTimer.start();
                runTimer.start();
                getTime(dateCommTimer, true);
                while (runTimer.elapsedSeconds() < warmUpTime) {
                    getTime(dateCommTimer, false);

                    runQueries(queryTarget, true, rngWarmup);
                }
            } catch (SQLException e) {
                Logger.LOG(threadName + ": SQL Exception during querying warmup.");
                e.printStackTrace();
            } catch (IOException e) {
                Logger.LOG(threadName + ": IO Exception during querying warmup.");
                e.printStackTrace();
            }
        }

        try {
            Logger.LOG(threadName + ": Queries have started.");
            if(duration > 0){
                dateCommTimer.start();
                runTimer.start();
                getTime(dateCommTimer, true);
                if(printProgressReports) reportTimer.start();
                while (runTimer.elapsedSeconds() < duration) {
                    getTime(dateCommTimer, false);

                    runQueries(queryTarget, false, rngQueries);

                    if(printProgressReports) progressReport(reportTimer, reportFrequency);
                }
            } else {
                assert targetCount > 0;
                dateCommTimer.start();
                getTime(dateCommTimer, true);
                if(printProgressReports) reportTimer.start();
                while (countFull < targetCount) {
                    getTime(dateCommTimer, false);

                    runQueries(queryTarget, false, rngQueries);

                    if(printProgressReports) progressReport(reportTimer, reportFrequency);
                }
            }
        } catch (SQLException e) {
            Logger.LOG(threadName + ": SQL Exception during querying.");
            e.printStackTrace();
        } catch (IOException e) {
            Logger.LOG(threadName + ": IO Exception during querying.");
            e.printStackTrace();
        }

        try {
            queryTarget.done();
        } catch (SQLException e) {
            Logger.LOG(threadName + ": SQL Exception during 'done' call.");
            e.printStackTrace();
        } catch (IOException e) {
            Logger.LOG(threadName + ": IO Exception during 'done' call.");
            e.printStackTrace();
        }
        Logger.LOG(() -> reportStats(true));

        if(saveQueryResults){
            saveQueryResults();
        }
    }

    private void progressReport(CoarseTimer timer, int frequency){
        if(timer.elapsedSeconds() > frequency){
            Logger.LOG(() -> reportStats(false));
            timer.start();
        }
    }

    private void getTime(PreciseTimer dateCommTimer, boolean force) throws IOException, SQLException{
        if(config.doDateCommunicationByQueryingDatabase()){
            if(config.getQueryDateCommunicationIntervalInMillisec() == 0 || force){ // On the first time-grab we dont want to wait for the timer.
                // The first time that we grab a value, our "newest valid date" is sourced from the config and is a
                // 'best guess' about what the actual value is. However, since it has no guaranteed relation to the actual
                // data, we cannot use it here because it may use that timestamp to improve query-performance to grab the
                // next timestamp value. So for the first call, use a timestamp value that we know will be older than
                // whatever is in the database
                unmodifiedNewestValidDate = queryTarget.getNewestTimestamp(LocalDateTime.ofEpochSecond(0,0, ZoneOffset.ofHours(0)));
                // The newest valid date stored in the database has whatever granularity that it was saved with.
                // This granularity may (depending on benchmark config settings) differ between databases
                // (e.g. Influx can store nanoseconds, while Kudu can store microseconds).
                // To get consistent query-intervals arguments across databases (and to simplify argument-generation),
                // time-interval generation uses second-granularity. This has the effect of truncating the returned
                // timestamp anyway which makes us miss the entries between the raw value and the truncated value.
                // To get everything, we truncate and then add a second to avoid this issue.
                newestValidDate = unmodifiedNewestValidDate.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
            } else if(dateCommTimer.elapsedSeconds() * 1000 > config.getQueryDateCommunicationIntervalInMillisec()){
                unmodifiedNewestValidDate = queryTarget.getNewestTimestamp(unmodifiedNewestValidDate);
                // The newest valid date stored in the database has whatever granularity that it was saved with.
                // This granularity may (depending on benchmark config settings) differ between databases
                // (e.g. Influx can store nanoseconds, while Kudu can store microseconds).
                // To get consistent query-intervals arguments across databases (and to simplify argument-generation),
                // time-interval generation uses second-granularity. This has the effect of truncating the returned
                // timestamp anyway which makes us miss the entries between the raw value and the truncated value.
                // To get everything, we truncate and then add a second to avoid this issue.
                newestValidDate = unmodifiedNewestValidDate.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
                dateCommTimer.start();
            }
        } else {
             newestValidDate = dateComm.getNewestTime();
        }
    }

    private void saveQueryResults(){
        Path path = Paths.get(config.DEBUG_saveQueryResultsPath());
        // Create a unique folder to save our config results in. Folder starting with '-' are inconvenient to cd into, so abs it.
        String folderName = Math.abs(config.getSettings().hashCode()) + "_T" + threadNumber;
        Path outPath = path.resolve(folderName);

        if (outPath.toFile().exists()){
            try {
                deleteDirectory(outPath);
            } catch (IOException e) {
                Logger.LOG(threadName + ": DEBUG: Error deleting dir " + outPath.toString());
                e.printStackTrace();
                assert false : threadName + ": Failed to delete old directory";
            }
        }

        boolean madeTargetDir = outPath.toFile().mkdirs();
        if(madeTargetDir){
            Logger.LOG(threadName + ": DEBUG: Writing query results to " + outPath);
        } else {
            Logger.LOG(threadName + ": DEBUG: Failed to create directory " + outPath + " to write query results to");
            assert false : threadName + ": Failed to create directory";
            return;
        }

        try {
            List<String> settingsPrint = new ArrayList<>();
            settingsPrint.add(config.getSettings());
            Files.write(outPath.resolve("config.txt"), settingsPrint, StandardCharsets.UTF_8);

            for (QueryResult result : queryResults){
                String filename = String.format("%04d_%s.csv", result.id, result.type);
                Path outFile = outPath.resolve(filename);
                List<String> content = new ArrayList<>();
                for(AbstractResult qRes : result.result){
                    content.add(qRes.toString());
                }
                Files.write(outFile, content, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            Logger.LOG(threadName + ": Error writing to dir " + outPath.toString());
            e.printStackTrace();
            assert false : threadName + ": Failed to write to directory";
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        if(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)){
            try (DirectoryStream<Path> values = Files.newDirectoryStream(path)) {
                for (Path value : values){
                    deleteDirectory(value);
                }
            }
        }
        Files.delete(path);
    }

    private enum QueryType{
        TotalClients, FloorTotals, MaxForAP, AvgOccupancy, KMeans, UNKNOWN
    }

    static class QueryResult{
        final int id;
        final QueryType type;
        final List<? extends AbstractResult> result;

        QueryResult(int id, QueryType type, List<? extends AbstractResult> result){
            this.id = id;
            this.type = type;
            this.result = result;
        }
    }
}