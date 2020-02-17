package Benchmark.Queries;

import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Logger;
import Benchmark.Queries.Results.KMeans;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class KMeansImplementation {
    private final List<AccessPoint> APs;
    private final FetchTimeSeries fetcher;
    private final int numIterations;

    private Map<Integer, Set<String>> assignments;
    private final int numClusters;

    public KMeansImplementation(int numIterations, int numClusters, AccessPoint[] APs, Random rng, FetchTimeSeries fetcher){
        this.numIterations = numIterations;
        this.APs = Arrays.asList(APs);
        this.fetcher = fetcher;

        // Our cluster initialization step is likely to choose bad cluster-locations which, when combined with our simplistic
        // distance-function, is likely to cause many empty clusters. To make this issue less prominent, we shuffle
        // our APs to introduce some randomness into the cluster-initialization. This isn't ideal, but much simpler
        // than implementing a better cluster-initialization algorithm such as 'K-Means++'
        Collections.shuffle(this.APs, rng);

        assert numClusters > 0;
        if(numClusters > this.APs.size()){
            Logger.LOG("INFO: Due to lazy cluster initialization, we cannot have more clusters than access points. " +
                    "However, there is no reason for why you might want that since the additional clusters would be empty anyway. " +
                    "The number of clusters has been set to " + this.APs.size() + " to match the number of generated APs");
            this.numClusters = this.APs.size();
        } else {
            this.numClusters = numClusters;
        }
    }

    public interface FetchTimeSeries{
        TimeSeries fetch(String AP) throws SQLException, IOException;
    }

    public List<KMeans> computeKMeans() throws IOException, SQLException {
        boolean allClustersInitialized = false;
        int clusterInitializationIndex = 0;
        TimeSeries[] clusters = new TimeSeries[numClusters];

        //TODO: Terminate early if we've converged?
        for(int p = 0; p < numIterations; p++){
            int[][] updatedCentroids = null;
            assignments = new HashMap<>();
            for(int i = 0; i < numClusters; i++){
                assignments.put(i, new HashSet<>());
            }

            // Loop through all data-points, assigning them to the closest cluster.
            for(AccessPoint AP : APs){
                TimeSeries series = fetcher.fetch(AP.getAPname());
                int bestCluster = -1;

                if(!allClustersInitialized){
                    // Without insight into the seed data we could place our initial clusters randomly in our dataset.
                    // However, this is practically guaranteed to produce horrible cluster locations because we have
                    // no info about the distribution of the generated data-points.
                    // Therefore we instead initialize the clusters to the first data-points that we come across. This
                    // doesn't produce great clusters, but they're not horrific either.
                    clusters[clusterInitializationIndex] = series;
                    bestCluster = clusterInitializationIndex;
                    clusterInitializationIndex++;
                    if(clusterInitializationIndex == numClusters) allClustersInitialized = true;
                } else {
                    int minDist = Integer.MAX_VALUE;
                    for(int i = 0; i < numClusters; i++){
                        TimeSeries cluster = clusters[i];
                        int distance = cluster.distanceTo(series);
                        //System.out.println("For " + AP.getAPname() + " the distance to cluster " + i + " is " + distance);
                        if(distance < minDist){
                            minDist = distance;
                            bestCluster = i;
                        }
                    }
                }

                // Delay initialization of this array-size until we know that we have initialized a cluster.
                if(updatedCentroids == null){
                    updatedCentroids = new int[numClusters][clusters[0].getNumValues()];
                }

                // We sum up the client-values of the APs that are assigned to the cluster as we assign them, so that
                // we dont have to pull the data back down from the database again later for the cluster-update step.
                int[] values = series.values;
                for(int i = 0; i < updatedCentroids[bestCluster].length; i++){
                    // Cluster or AP is missing values... what would a 'proper' implementation do here?
                    if(i >= values.length) break;

                    updatedCentroids[bestCluster][i] += values[i];
                }

                assignments.get(bestCluster).add(AP.getAPname());
            }

            // Update clusters to the mean of their assigned points.
            for(int i = 0; i < numClusters; i++){
                assert updatedCentroids != null;
                int[] clusterVals = updatedCentroids[i];
                int numberOfPointsAssignedToCluster = assignments.get(i).size();
                if(numberOfPointsAssignedToCluster == 0){
                    // This cluster has no assigned points. What should we do with the now-empty cluster?
                    // We're going the easy route of keeping it around and not moving it.
                    updatedCentroids[i] = clusters[i].values;
                } else {
                    for(int j = 0; j < clusterVals.length; j++){
                        clusterVals[j] = clusterVals[j] / numberOfPointsAssignedToCluster;
                    }
                }

                //System.out.println("Number of points assigned to cluster " + i + " is " + numberOfPointsAssignedToCluster);
                clusters[i] = new TimeSeries(clusters[i].timestamps, updatedCentroids[i]);
            }
        }

        List<KMeans> output = new ArrayList<>(numClusters);
        for(int i = 0; i < numClusters; i++){
            output.add(new KMeans(assignments.get(i)));
        }

        return output;
    }

    public static class TimeSeries{
        private final Instant[] timestamps;
        private final int[] values;

        //NOTE: We force the caller to populate the timestamp-array to ensure that we pull all the data that we would
        //      need for a proper distance-function implementation, but we dont actually make use of the timestamps in
        //      the naive distance implementation.
        public TimeSeries(Instant[] timestamps, int[] values){
            this.timestamps = timestamps;
            this.values = values;
        }

        //TODO: This distance function doesn't really take into account when data is missing from one time-series but not
        //      the other one. The intent of this K-Means implementation is to give insight into the performance-characteristics
        //      of the database and we therefore dont really care about the cluster-results that K-Means gives us. Any cluster
        //      distribution will give the same database access-pattern.
        //      This simplistic distance-function is therefore fine for our use-case. It could be improved, but a 'good'
        //      implementation would use a proper distance-calculation such as 'dynamic time warping' instead of the
        //      pair-wise distance between points in the time-series that the current implementation uses..
        int distanceTo(TimeSeries other){
            int dist = 0;

            int i = 0;
            int j = 0;

            while(true){
                if(i == values.length){
                    for(; j < other.values.length; j++){
                        dist += other.values[j];
                    }
                    break;
                }
                if(j == other.values.length){
                    for(; i < values.length; i++){
                        dist += values[i];
                    }
                    break;
                }

                int currentValue = values[i];
                int otherValue = other.values[j];

                dist += Math.abs(currentValue - otherValue);

                i++;
                j++;
            }

            return dist;
        }

        public int getNumValues(){
            return values.length;
        }
    }
}
