import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

public class G039HW1{


    public static void main(String[] args) throws IOException {
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: file_path D M K L");
        }

        // Parameters parsing

        // Read the file

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

    public static void MRApproxOutliers(JavaRDD<Point> points, float D, int M, int K){
        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        // Step B: Compute N3 and N7 for each cell
    }
}