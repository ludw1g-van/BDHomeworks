import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class G039HW2{


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
        SparkConf conf = new SparkConf(true).setAppName("Clustering");
        JavaSparkContext sc = new JavaSparkContext(conf);
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
        System.out.println("Number of points = " + inputPoints.count());

        //  Farthest-First Traversal algorithm through standard sequential code
        ArrayList<Integer> C = new ArrayList<Integer>();
        ArrayList<Tuple2<Float, Float>> listOfPoints = new ArrayList<>(inputPoints.collect());
        C = SequentialFFT(listOfPoints, K);

        // MRFFT: 3 round MapReduce algorithm
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

    public static ArrayList<Integer> SequentialFFT(ArrayList<Tuple2<Float, Float>> points, int K){

        //SequentialFFT takes in input a set P of points and an integer parameter K,
        //and must return a set C of K centers.
        //Both p and C must be represented as lists (ArrayList in Java and list in Python).
        //The implementation should run in O(|P|⋅K) time.

        return null;
    }

    public static float MRFFT(JavaPairRDD<Float, Float> points, int K){

        //Rounds 1 and 2 compute a set C of K centers, using the MR-FarthestFirstTraversal algorithm described in class.
        // The coreset computed in Round 1, must be gathered in an ArrayList in Java and,
        // in Round 2, the centers are obtained by running SequentialFFT on the coreset.

        //Round 3 computes and returns the radius R of the clustering induced by the centers,
        //that is the maximum, over all points x∈P, of the distance dist(x,C).
        //The radius R must be a float. To compute R you cannot download P
        //into a local data structure, since it may be very large, and must keep it stored as an RDD.
        // However, the set of centers C computed in Round 2, can be used as a global variable.
        //To this purpose we ask you to copy C into a broadcast variable
        // which can be accessed by the RDD methods that will be used to compute R.

        //MRFFT must compute and print, separately, the running time required by each of the above 3 rounds.

        return 0;
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