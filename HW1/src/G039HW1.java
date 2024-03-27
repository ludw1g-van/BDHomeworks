import java.io.IOException;
import java.util.*;
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
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);
        System.out.println(path + " D=" + D + " M=" + M + " K=" + K + " L=" + L);

        // SPARK SETUP
        SparkConf conf = new SparkConf(true).setAppName("OutlierDetection");
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
        System.out.println("Number of points = " + inputPoints.count());

        long start, stop;
        //Only if the number of points is at most 200000:
        //Downloads the points into a list called listOfPoints
        if(inputPoints.count() <= 200000){
            ArrayList<Tuple2<Float, Float>> listOfPoints = new ArrayList<>(inputPoints.collect());
            //Executes ExactOutliers with parameters listOfPoints,  D,M and K
            start = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
            stop = System.currentTimeMillis();

            //Prints ExactOutliers' running time.
            System.out.printf("Running time of ExactOutliers = %d ms\n", stop - start);
        }

        //In all cases:
        //Executes MRApproxOutliers with parameters inputPoints, D, M and K

        start = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M, K);
        stop = System.currentTimeMillis();

        //Prints MRApproxOutliers' running time.
        System.out.printf("Running time of MRApproxOutliers = %d ms\n", stop - start);

        sc.close();
    }

    public static void ExactOutliers(ArrayList<Tuple2<Float, Float>> points, float D, int M, int K){
        int outliersCount = 0;
        HashMap<Tuple2<Float, Float>, Integer> outlierNeighCount = new HashMap<>();
        for (Tuple2<Float, Float> point1 : points) {
            int pointsInsideTheBall = 0;
            for (Tuple2<Float, Float> point2 : points) {

                // Compute the distance
                double distance = Math.sqrt(Math.pow(point2._1 - point1._1, 2) + Math.pow(point2._2 - point1._2, 2));

                // The point2 is in the ball of radios D (B(point1, D))
                if (distance <= D){
                    pointsInsideTheBall++;
                }
            }

            //System.out.print("The point (" + point1._1() + ", " +  point1._2() + ")" + " is an ");

            // If the point is an outlier add it to the outlier neighbour list and increment the outliers count
            if(pointsInsideTheBall <= M){
                outlierNeighCount.put(new Tuple2<>(point1._1, point1._2), pointsInsideTheBall);
                outliersCount++;
                //System.out.print("OUTLIER\n");
            }
        }

        // Print the outlier count
        System.out.println("Number of Outliers = " + outliersCount);
        //Sorting the hashmap
        HashMap<Tuple2<Float, Float>, Integer> sortedNeighbourCount = outlierNeighCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        // Print the sorted hashMap (only the first K elements)
        int count = 0;
        for (Map.Entry<Tuple2<Float, Float>, Integer> entry : sortedNeighbourCount.entrySet()) {
            System.out.println("Point: " + "(" + entry.getKey()._1() + "," + entry.getKey()._2() + ")");
            count++;
            if (count >= K) {
                break;
            }
        }

    }

    public static void MRApproxOutliers(JavaPairRDD<Float, Float> points, float D, int M, int K){

        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellSize;
        cellSize = points.mapToPair((document) -> {    // <-- MAP PHASE (R1) - (x_i, y_i) --> ((i,j), 1)
            Tuple2<Tuple2<Integer, Integer>, Integer> pair;

            // Cell coordinates
            double side =  (D/(2*Math.sqrt(2)));
            int i = (int) Math.floor(document._1()/side);
            int j = (int) Math.floor(document._2()/side);
//            System.out.printf("(%f, %f) --> (%d, %d) side=%f\n", document._1(), document._2(), i, j, side);
            pair = new Tuple2<>(new Tuple2<>(i,j), 1);
//            System.out.println("Call to map to pair " + document);
            return pair;
        }).reduceByKey((x,y) -> {
//            System.out.println("Call to re by key " + x + " " + y);
            return x + y;
        });


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

        //The first K non-empty cells,  in non-decreasing order of cell size, printing, for each such cell, its identifier and its size.
        cellSize.mapToPair((el) -> new Tuple2<>(el._2(), el._1()))
                .sortByKey()
                .take(K)
                .forEach((el) -> {
                    System.out.printf("Cell: (%d,%d)  Size = %d\n", el._2()._1(), el._2()._2(), el._1());
                });
    }
}