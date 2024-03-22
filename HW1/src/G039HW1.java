import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;

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

    public static void ExactOutliers(ArrayList<Point> points){
        // Double for loop
    }

    public static void MRApproxOutliers(JavaRDD<Point> points, float D, int M, int K){
        // Step A: Transform input RDD into an RDD of non-empty cells with their point counts
        // Step B: Compute N3 and N7 for each cell
    }
}