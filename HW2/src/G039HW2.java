import java.io.IOException;
import java.util.*;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class G039HW2{

    /**
     * Spark context
     */
    static JavaSparkContext sc;

    /**
     * Partition number
     */
    static int L;

    public static void main(String[] args) throws IOException {


        // Checking the number of command-line arguments
        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: file_path M K L");
        }

        SparkConf conf = new SparkConf(true).setAppName("Clustering");
        conf.set("spark.locality.wait", "0s");

        //init spark context
        sc = new JavaSparkContext(conf);

        // Prints the command-line arguments and stores M,K,L
        String path = args[0];
        int M = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        L = Integer.parseInt(args[3]);
        System.out.println(path + " M=" + M + " K=" + K + " L=" + L);

        // set Spark log level
        sc.setLogLevel("WARN");


        // Input reading and partitioning
        JavaRDD<String> rawData = sc.textFile(path);
        JavaPairRDD<Float, Float> inputPoints = rawData.mapToPair(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        }).persist(StorageLevel.MEMORY_AND_DISK());


        // Print the total number of points
        long pointCount = inputPoints.count();
        System.out.println("Number of points = " + pointCount);

        //Check that K is less than the number of points
        if(K > pointCount){
            throw new IllegalArgumentException("K should be <= than the number of points");
        }


        // MRFFT
        float D = MRFFT(inputPoints, K);

        // print the radius
        System.out.println("Radius = " + D);

        // variables to get the execution time of MRApproxOutliers
        long start, stop;

        // executes MRApproxOutliers
        start = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M);
        stop = System.currentTimeMillis();

        // Print MRApproxOutliers running time
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
    public static ArrayList<Tuple2<Float, Float>> SequentialFFT(final ArrayList<Tuple2<Float, Float>> points, final int K){
        // The array of centers that will be returned
        Tuple2<Float, Float>[] centers = new Tuple2[K];

        // HashMap to save the computed distances d(x, S) where x is a point in P-S and S is the set of centers
        HashMap<Tuple2<Float, Float>, Float> minDistances = new HashMap<>();

        //Add the first center
        centers[0] = points.get(0);

        // number of centers in the array
        int centersNum = 1;

        for(int i=2; i <= K; i++){

            // Max distance and maxDistance point
            Tuple2<Float, Float> maxDistPoint = null;
            float maxDistance = 0F;

            for(Tuple2<Float, Float> point : points){

                // Get the last inserted center
                Tuple2<Float, Float> lastInsertedCenter = centers[centersNum - 1];

                // If the point is in the center set, mark it with a negative minDistance
                if(point == lastInsertedCenter){
                    minDistances.put(point, -1F);
                }

                // Get the minimum distance calculated before
                Float prevDistance = minDistances.get(point);

                // Skip the centers
                if(prevDistance != null && prevDistance == -1) continue;

                // Calculate the Euclidean distance squared
                float distance = (((lastInsertedCenter._1 - point._1)*(lastInsertedCenter._1 - point._1)) + ((lastInsertedCenter._2 - point._2)*(lastInsertedCenter._2 - point._2)));

                // Store the minimum distance (the way of calculating d(x, S))
                if(prevDistance == null || prevDistance > distance){
                        minDistances.put(point, distance);
                }

                float currentMinDistance = minDistances.get(point);
                //update maxDistance and maxDistance Point
                if(maxDistance < currentMinDistance){
                    maxDistance = currentMinDistance;
                    maxDistPoint = point;
                }
            }

            // add the found center to the list of centers
            centers[centersNum++] = maxDistPoint;

        }
        return new ArrayList<>(Arrays.asList(centers));
    }

    /**
     * R1 -> corset
     * R2 -> k centers (transform the array list into a Broadcast to ship the data structure
     * to define it you need the spark context, therefore the spark context should be an object variable (static))
     * R3 -> radius
     *
     * @param points input set of points.
     * @param K number of centers.
     * @return the radius of the clustering induced by the centers.
     */
    public static float MRFFT(JavaPairRDD<Float, Float> points, int K){

        // start and stop variables to store the time.
        long start, stop;

        //ROUND 1
        start = System.currentTimeMillis();
        JavaRDD<Tuple2<Float, Float>> result = points.repartition(L).mapPartitions(partition -> {
            ArrayList<Tuple2<Float, Float>> tempList = new ArrayList<>();
            while(partition.hasNext()) tempList.add(partition.next());
            return SequentialFFT(tempList, K).iterator();
        }).persist(StorageLevel.MEMORY_AND_DISK());
        result.count();
        stop = System.currentTimeMillis();
        System.out.printf("Running time of MRFFT Round 1 = %d ms\n", stop - start);

        //ROUND 2
        start = System.currentTimeMillis();
        ArrayList<Tuple2<Float, Float>> coreset = new ArrayList<>(result.collect());
        ArrayList<Tuple2<Float, Float>> C = SequentialFFT(coreset, K); // result of the FFT algorithm on the coreset
        stop = System.currentTimeMillis();
        System.out.printf("Running time of MRFFT Round 2 = %d ms\n", stop - start);

        // ROUND 3
        Broadcast<ArrayList<Tuple2<Float, Float>>> sharedCenters = sc.broadcast(C);
        start = System.currentTimeMillis();
        float R = clustering(sharedCenters, points);
        stop = System.currentTimeMillis();
        System.out.printf("Running time of MRFFT Round 3 = %d ms\n", stop - start);

        sharedCenters.destroy();
        return R;
    }

    /**
     * Computes and returns the radius R of the clustering induced by the centers,
     * that is the maximum, over all points x in P of the distance dist(x,C).
     *
     * @param sharedCenters broadcast variable containing the centers of the cluster.
     * @param points input set of points.
     * @return radius of the clustering induced by the centers.
     */
    public static float clustering(Broadcast<ArrayList<Tuple2<Float, Float>>> sharedCenters, JavaPairRDD<Float, Float> points) {
        return points.map(point -> {
            ArrayList<Tuple2<Float, Float>> centers = sharedCenters.value();
            float minDistance = Float.MAX_VALUE;
            for (Tuple2<Float, Float> center : centers) {
                float distance = (float) (Math.pow((point._1() - center._1()), 2) + Math.pow((point._2() - center._2()), 2));
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            return (float) Math.sqrt(minDistance);
        }).reduce(Math::max);
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