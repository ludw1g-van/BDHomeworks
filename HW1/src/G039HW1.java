import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class G039HW1{


    public static void main(String[] args) throws IOException {

        // checking the number of command-line arguments
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: file_path D M K L");
        }

        //Prints the command-line arguments and stores D,M,K,L
        // into suitable variables
        String path = args[0];
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);
        for(int i = 0; i< args.length; i++) {
            System.out.printf("Command Line Argument %d is %s%n", i, args[i]);
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("OutlineDetection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");



        // INPUT READING
        //Reads the input points into an RDD of strings (called rawData)
        // and transform it into an RDD of points (called inputPoints),
        // represented as pairs of floats, subdivided into L partitions.
        JavaRDD<String> rawData = sc.textFile(path); //sc.textFile(args[0])
        JavaPairRDD<Float, Float> inputPoints = rawData.mapToPair(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        }).repartition(L).cache();


        //Prints the total number of points.
        System.out.println(inputPoints.count());

        //Only if the number of points is at most 200000:
        //Downloads the points into a list called listOfPoints
        if(inputPoints.count() <= 200000){
            ArrayList<Tuple2<Float, Float>> listOfPoints = new ArrayList<>(inputPoints.collect());
            //Executes ExactOutliers with parameters listOfPoints,  D,M and K
            //The execution will print the information specified above.
            //Prints ExactOutliers' running time.

            ExactOutliers(listOfPoints, D, M, K);
        }

        //In all cases:
        //Executes MRApproxOutliers with parameters inputPoints, D,M
        // and K
        //. The execution will print the information specified above.
        //Prints MRApproxOutliers' running time.

        //MRApproxOutliers(inputPoints, D, M, K);

        //File OutputFormat.txt (TO BE ADDED) shows you how to format your output. Make sure that your program complies with this format.

        sc.close();

    }

    public static void ExactOutliers(ArrayList<Tuple2<Float, Float>> points, float D, int M, int K){
        int outliersCount = 0;
        HashMap<Tuple2<Float, Float>, Integer> outlierNeighCount = new HashMap<>();
        for (Tuple2<Float, Float> point1 : points) {
            int pointsInsideTheBall = 0;
            for (Tuple2<Float, Float> point2 : points) {

                // Don't compute the distance for the same point
                if(point1 == point2) continue;

                // Compute the distance
                double distance = Math.sqrt(Math.pow(point2._1 - point1._1, 2) + Math.pow(point2._2 - point1._2, 2));

                // The point2 is in the ball of radios D (B(point1, D))
                if (distance <= D){
                    pointsInsideTheBall++;
                }
            }

            System.out.print("The point (" + point1._1 + ", " +  point1._2 + ")" + " is an ");

            // If the point is an outlier add it to the outlier neighbour list and increment the outliers count
            if(pointsInsideTheBall <= M){
                outlierNeighCount.put(new Tuple2<>(point1._1, point1._2), pointsInsideTheBall);
                outliersCount++;
                System.out.print("OUTLIER\n");
            } else {
                System.out.print("non-OUTLIER\n");
            }
        }

        // Print the outlier count
        System.out.println("Outliers count: " + outliersCount);

        //Sorting the hashmap
        HashMap<Tuple2<Float, Float>, Integer> sortedNeighbourCount = outlierNeighCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        // Print the sorted hashMap (only the first K elements)
        int count = 0;
        for (Map.Entry<Tuple2<Float, Float>, Integer> entry : sortedNeighbourCount.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
            count++;
            if (count >= K) {
                break;
            }
        }

    }

    public static void MRApproxOutliers(JavaPairRDD<Float, Float> points, float D, int M, int K){
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellSize;
        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        //cellSize = points.flatMapToPair((document) -> {    // <-- MAP PHASE (R1) - (x_i, y_i) --> ((i,j), 1)

        //}).mapPartitionsToPair((element) -> {    // <-- REDUCE PHASE (R1)

        //}).reduceBykey((x,y) -> {   // <-- REDUCE PHASE (R2)

        //});

        // Step B: Compute N3 and N7 for each cell
        //cellSize.flatMapToPair((element) -> {

        //}).flatMapToPair((element) -> { //NO SHUFFLING NEEDED

        //});

    }
}