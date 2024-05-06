import java.io.IOException;
import java.util.*;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class G039HW2{

    static SparkConf conf = new SparkConf(true).setAppName("Clustering");
    static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) throws IOException {

        // Checking the number of command-line arguments
        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: file_path M K L");
        }

        // Prints the command-line arguments and stores M,K,L
        String path = args[0];
        int M = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);
        System.out.println(path + " M=" + M + " K=" + K + " L=" + L);

        // SPARK SETUP
        //SparkConf conf = new SparkConf(true).setAppName("Clustering");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");


        // INPUT READING
        JavaRDD<String> rawData = sc.textFile(path);
        JavaPairRDD<Float, Float> inputPoints = rawData.mapToPair(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        }).repartition(L).cache();


        // Print the total number of points
        long pointCount = inputPoints.count();
        System.out.println("Number of points = " + pointCount);

        //Check that K is less than the number of points
        if(K > pointCount){
            throw new IllegalArgumentException("K should be <= than the number of points");
        }

        //  Farthest-First Traversal algorithm through standard sequential code
        ArrayList<Tuple2<Float, Float>> C;
        ArrayList<Tuple2<Float, Float>> listOfPoints = new ArrayList<>(inputPoints.collect());
        //System.out.println(listOfPoints);
        C = SequentialFFT(listOfPoints, K);
        System.out.println(C);

        // MR FFT: 3 round MapReduce algorithm
        float D;
        D = MRFFT(inputPoints, K);

        // MRApproxOutliers HW1
        long start, stop;

        start = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M);
        stop = System.currentTimeMillis();

        // Print MRApproxOutliers running time.
        System.out.printf("Running time of MRApproxOutliers = %d ms\n", stop - start);

        sc.close();
    }

    /**
     * SequentialFFT takes in input a set P of points and an integer parameter K,
     * and must return a set C of K centers.
     * Both p and C must be represented as lists (ArrayList in Java and list in Python).
     * The implementation should run in O(|P|⋅K) time.
     * @param points the list of points
     * @param K the number of centers
     * @return the list of K centers
     */
    public static ArrayList<Tuple2<Float, Float>> SequentialFFT(ArrayList<Tuple2<Float, Float>> points, int K){
        ArrayList<Tuple2<Float, Float>> centers = new ArrayList<>();

        // HashMap to save the computed distances d(x, S) where x is a point in P-S and S is the set of centers
        HashMap<Tuple2<Float, Float>, Double> minDistances = new HashMap<>();

        //Add the first center
        centers.add(points.get(0));
        for(int i=2; i <= K; i++){

            // Max distance and maxDistance point
            Tuple2<Float, Float> maxDistPoint = null;
            Double maxDistance = 0.0;

            for(Tuple2<Float, Float> point : points){

                // Get the last inserted center
                Tuple2<Float, Float> lastInsertedCenter = centers.get(centers.size()-1);

                // If the point is in the center set, mark it with a negative minDistance
                if(point == lastInsertedCenter){
                    minDistances.put(point, -1.0);
                }

                // Get the minimum distance calculated before
                Double prevDistance = minDistances.get(point);

                // Skip the centers
                if(prevDistance != null && prevDistance == -1) continue;

                // Calculate the Euclidean distance squared
                double distance = Math.pow((lastInsertedCenter._1() - point._1()), 2) + Math.pow((lastInsertedCenter._2() - point._2()), 2);

                // Store the minimum distance (the way of calculating d(x, S))
                if(prevDistance == null || prevDistance > distance){
                    minDistances.put(point, distance);
                }

                //update maxDistance and maxDistance Point
                if(maxDistance < minDistances.get(point)){
                    maxDistance = minDistances.get(point);
                    maxDistPoint = point;
                }
            }

            // add the found center to the list of centers
            centers.add(maxDistPoint);
        }
        return centers;
    }

    /**
     * R1 -> corset
     * R2 -> k centers (transform the array list into a Broadcast to ship the data structure
     * to define it you need the spark context, therefore the spark context should be an object variable (static))
     * R3 -> radius
     *
     * @param points
     * @param K
     * @return
     */
    public static float MRFFT(JavaPairRDD<Float, Float> points, int K){

        //Rounds 1 and 2 compute a set C of K centers, using the MR-FarthestFirstTraversal algorithm described in class.
        // The corset computed in Round 1, must be gathered in an ArrayList in Java and,
        // in Round 2, the centers are obtained by running SequentialFFT on the corset.

        ///////////////////////////
        // ROUND 1
        ///////////////////////////
        long start, stop;
        start = System.currentTimeMillis();

        // MAP PHASE R1
        // Count the total number of points
        long totalPoints = points.count();
        int numPartitions = 5; // You can set the number of partitions
        long pointsPerPartition = totalPoints / numPartitions;

        // Assign sequential indices and then partition keys to each point
        JavaPairRDD<Integer, Tuple2<Float, Float>> indexedPoints = points.zipWithIndex()
                .mapToPair(t -> new Tuple2<>((int)(t._2 / pointsPerPartition), t._1));

        // Group points by partition key
        JavaPairRDD<Integer, Iterable<Tuple2<Float, Float>>> partitionedPoints = indexedPoints.groupByKey();

        // Output the grouped points to check the partitions
        ArrayList<Tuple2<Integer, Iterable<Tuple2<Float, Float>>>> partitions = new ArrayList<>(partitionedPoints.collect());
        /*for (Tuple2<Integer, Iterable<Tuple2<Float, Float>>> partition : partitions) {
            System.out.println("Partition Key: " + partition._1);
            for (Tuple2<Float, Float> point : partition._2) {
                System.out.println(point);
            }
            System.out.println("---");
        }*/

        // REDUCE PHASE R1
        JavaRDD<ArrayList<Tuple2<Float, Float>>> corset = partitionedPoints.map((partition) -> {
            ArrayList<Tuple2<Float, Float>> centersCorset;
            ArrayList<Tuple2<Float, Float>> partitionToList = new ArrayList<>();

            // Iterate through the Iterable
            if (partition != null) {
                for (Tuple2<Float, Float> item : partition._2()) {
                    partitionToList.add(item);
                }
            }
            centersCorset = SequentialFFT(partitionToList, K);
            return centersCorset;
        });

        stop = System.currentTimeMillis();
        System.out.printf("Running time of ROUND 1 = %d ms\n", stop - start);

        ArrayList<List<Tuple2<Float, Float>>> corsets = new ArrayList<>(corset.collect());
        /*for (List<Tuple2<Float, Float>> c : corsets) {
            for (Tuple2<Float, Float> point : c) {
                System.out.println(point);
            }
            System.out.println("---");
        }*/
        ///////////////////////////
        // ROUND 2
        ///////////////////////////
        start = System.currentTimeMillis();

        ArrayList<Tuple2<Float, Float>> centers = new ArrayList<>();
        for (List<Tuple2<Float, Float>> c: corsets) {
            centers = SequentialFFT((ArrayList<Tuple2<Float, Float>>) c, K);
        }
        Broadcast<ArrayList<Tuple2<Float, Float>>> sharedCenters = sc.broadcast(centers);

        stop = System.currentTimeMillis();
        System.out.printf("Running time of ROUND 2 = %d ms\n", stop - start);

        ///////////////////////////
        // ROUND 3
        ///////////////////////////
        start = System.currentTimeMillis();

        float R = clustering(sharedCenters, points);

        stop = System.currentTimeMillis();
        System.out.printf("Running time of ROUND 3 = %d ms\n", stop - start);

        //Round 3 computes and returns the radius R of the clustering induced by the centers,
        //that is the maximum, over all points x∈P, of the distance dist(x,C).
        //The radius R must be a float. To compute R you cannot download P
        //into a local data structure, since it may be very large, and must keep it stored as an RDD.
        //However, the set of centers C computed in Round 2, can be used as a global variable.
        //To this purpose we ask you to copy C into a broadcast variable
        //which can be accessed by the RDD methods that will be used to compute R.

        sharedCenters.destroy();
        //MR-FFT must compute and print, separately, the running time required by each of the above 3 rounds.
        return R;
    }

    public static float clustering(Broadcast<ArrayList<Tuple2<Float, Float>>> sharedCenters, JavaPairRDD<Float, Float> points) {

        JavaRDD<Float> minDistances = points.map(point -> {
            ArrayList<Tuple2<Float, Float>> centers = sharedCenters.value();
            float minDistance = Float.MAX_VALUE;
            for (Tuple2<Float, Float> center : centers) {
                float distance = (float) (Math.pow((center._1() - point._1()), 2) + Math.pow((center._2() - point._2()), 2));
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            return minDistance;
        });

        return minDistances.reduce(Math::max);
    }

    public static void MRApproxOutliers(JavaPairRDD<Float, Float> points, float D, int M){

        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellSize;

        cellSize = points.mapToPair((point) -> {    // <-- MAP PHASE (R1) - (x_i, y_i) --> ((i,j), 1)
            Tuple2<Tuple2<Integer, Integer>, Integer> pair;

            // Cell coordinates
            double side =  (D/(2*Math.sqrt(2)));
            int i = (int) Math.floor(point._1()/side);
            int j = (int) Math.floor(point._2()/side);
            pair = new Tuple2<>(new Tuple2<>(i,j), 1);
            return pair;
        }).reduceByKey(Integer::sum);


        // Step B: Compute N3 and N7 for each cell
        HashMap<Tuple2<Integer, Integer>,Integer> nonEmptyCells = new HashMap<>(cellSize.collectAsMap());
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> cellInfoRDD = cellSize.mapToPair(cell -> {
            final int i = cell._1()._1();
            final int j = cell._1()._2();
            int N3 = 0;
            int N7 = 0;

            // Calculate N3
            for (int l = i - 1; l <= i + 1; l++) {
                for (int m = j - 1; m <= j + 1; m++) {
                    Tuple2<Integer, Integer> tmpPoint = new Tuple2<>(l,m);
                    if (nonEmptyCells.get(tmpPoint) != null) {
                        N3 += nonEmptyCells.get(tmpPoint);
                    }
                }
            }

            // Calculate N7
            for (int l = i - 3; l <= i + 3; l++) {
                for (int m = j - 3; m <= j + 3; m++) {
                    Tuple2<Integer, Integer> tmpPoint = new Tuple2<>(l,m);
                    if (nonEmptyCells.get(tmpPoint) != null) {
                        N7 += nonEmptyCells.get(tmpPoint);
                    }
                }
            }

            return new Tuple2<>(cell._1(), new Tuple2<>(N3, N7));
        });


        // Collect information and print results
        ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> cellInfoList = new ArrayList<>(cellInfoRDD.collect());
        int sureOutliers = 0;
        int uncertainPoints = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> cellInfo : cellInfoList) {
            int N3 = cellInfo._2()._1();
            int N7 = cellInfo._2()._2();
            if (N7 <= M) {
                sureOutliers += nonEmptyCells.get(cellInfo._1());
            } else if (N3 <= M){
                uncertainPoints += nonEmptyCells.get(cellInfo._1());
            }
        }

        System.out.println("Number of sure outliers = " + sureOutliers);
        System.out.println("Number of uncertain points = " + uncertainPoints);
    }
}
