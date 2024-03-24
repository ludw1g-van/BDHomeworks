import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

        //Prints the command-line arguments and stores D,M,K,L
        // into suitable variables
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);
        for(int i = 0; i< args.length; i++) {
            System.out.println(String.format("Command Line Argument %d is %s", i, args[i]));
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("OutlineDetection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        String path = "TestN15-input.txt";

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
        ArrayList<List<Tuple2<Float, Float>>> listOfPoints = new ArrayList<>();
        if(inputPoints.count() < 20000){
            for (int i = 0; i < inputPoints.count(); i++) {
                listOfPoints.add(inputPoints.collect());
            }
            //Executes ExactOutliers with parameters listOfPoints,  D,M and K
            //The execution will print the information specified above.
            //Prints ExactOutliers' running time.

            //ExactOutliers(listOfPoints, D, M, K);
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

    public static void ExactOutliers(ArrayList<List<Tuple2<Float, Float>>> points, float D, int M, int K){
        // Double for loop
    }

    public static void MRApproxOutliers(JavaPairRDD<Float, Float> points, float D, int M, int K){
        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        // Step B: Compute N3 and N7 for each cell
    }
}