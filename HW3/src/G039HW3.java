import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class G039HW3{
    public static void main(String[] args) throws Exception {

        // Checking the number of command-line arguments
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: n phi epsilon delta portExp");
        }

        // Prints the command-line arguments and stores n, phi, epsilon, delta, portExp
        int n = Integer.parseInt(args[0]);
        float phi = Float.parseFloat(args[1]);
        float epsilon = Float.parseFloat(args[2]);
        float delta = Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);
        System.out.println("n=" + n + " phi=" + phi + " epsilon=" + epsilon + " delta=" + delta + " portExp=" + portExp);

        // Arguments validation
        if(!(phi > 0 && phi < 1)){
            throw new IllegalArgumentException("The argument phi must be in (0,1)");
        }
        if(!(epsilon > 0 && epsilon < 1)){
            throw new IllegalArgumentException("The argument epsilon must be in (0,1)");
        }
        if(!(delta > 0 && delta < 1)){
            throw new IllegalArgumentException("The argument delta must be in (0,1)");
        }

        // SPARK SETUP
        SparkConf conf = new SparkConf(true).setAppName("StickySampling");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        sc.close();
    }
}