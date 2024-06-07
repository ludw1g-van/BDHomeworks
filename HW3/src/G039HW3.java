import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class G039HW3{
    public static void main(String[] args) throws Exception {

        // Checking the number of command-line arguments
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: n phi epsilon delta portExp");
        }

        // Prints the command-line arguments and stores n, phi, epsilon, delta, portExp
        int n = Integer.parseInt(args[0]);
        float phi = java.lang.Float.parseFloat(args[1]);
        float epsilon = java.lang.Float.parseFloat(args[2]);
        float delta = java.lang.Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);
        System.out.println("INPUT PROPERTIES");
        System.out.println("n = " + n + " phi = " + phi + " epsilon = " + epsilon + " delta = " + delta + " port = " + portExp);

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
        if(!(portExp >= 8886 && portExp <= 8889)){
            throw new IllegalArgumentException("The argument portExp must be in [8886, 8889]");
        }

        // SPARK SETUP
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName("StickySampling");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore. The
        // main thread will first acquire the only permit available, and then it will try
        // to acquire another one right after spinning up the streaming computation.
        // The second attempt at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met the semaphore is released, basically giving "green light" to the main
        // thread to shut down the computation.

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Stream length (an array to be passed by reference)
        long[] streamLength = new long[1];
        streamLength[0] = 0L;

        // Hash Table for the distinct elements
        HashMap<Long, Long> histogram = new HashMap<>();

        // Size m of the Reservoir Sampling
        int m = (int) Math.ceil(1 / phi);

        // The size m of the m-sample S (stored in reservoir ArrayList)
        ArrayList<Long> reservoir = new ArrayList<>(m);

        // Atomic integer used to keep track of the instant of time (also between batches)
        AtomicInteger t = new AtomicInteger(0);

        // Randomizer for generating probabilities (used in Reservoir sampling)
        Random random = new Random();

        // Hash Table of the Sticky Sampling
        HashMap<Long, Long> S = new HashMap<>();

        // Counter of number of batches
        AtomicInteger countBatch = new AtomicInteger(0);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PROCESS PHASE (TRUE FREQUENCIES, RESERVOIR, EPSILON-AFI STICKY SAMPLING)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.

                    // Check whether the streaming has exceeded the limit
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        System.out.println("Batch Count: " + streamLength[0]);
                        //1-The true frequent items with respect to the threshold phi

                        // Map obtained by counting how many times a given number appear in the stream batch
                        Map<Long, Long> batchCounts = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey(Long::sum)
                                .collectAsMap();

                        // For each key appearing in my batch, add a new entry in the histogram
                        // for counting how many instances of a number appear in the whole streaming
                        for (Map.Entry<Long, Long> pair : batchCounts.entrySet()) {
                            histogram.compute(pair.getKey(), (k, count) -> count == null? pair.getValue() : count + pair.getValue());
                        }

                        //2-An m-sample of Σ using Reservoir Sampling of, with m= ⌈1/phi⌉

                        // Given a batch, foreachPartition work on an iterator of the batch RDD of strings.
                        batch.foreachPartition(items -> {

                            // Run until the batch is finished
                            while(items.hasNext()){

                                // If the instant of time is less than m, add the element to the m-sample
                                if(t.intValue() <= reservoir.size()){
                                    reservoir.add(Long.parseLong(items.next()));
                                } else {

                                    // Probability of getting a substitution of an element in the m-sample with the current one
                                    // N.B.: the probability decreases with time t (to keep uniform probability)
                                    float probThreshold = (float) reservoir.size() / t.intValue();

                                    // Randomized probability
                                    float prob = random.nextFloat();

                                    // Randomized index of the m-sample
                                    int randIndex = random.nextInt(reservoir.size());

                                    // The potentially substituted element in the m-sample
                                    Long substituted = reservoir.get(randIndex);

                                    // Check whether perform substitution and do it eventually
                                    if(prob <= probThreshold){
                                        reservoir.remove(substituted);
                                        reservoir.add(Long.parseLong(items.next()));
                                    }
                                }
                                t.incrementAndGet();
                            }
                        });

                        if (batchSize > 0) {
                            System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                        }
                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }

                    //3-The epsilon-Approximate Frequent Items computed using Sticky Sampling with confidence parameter delta

//                    countBatch.incrementAndGet();
//
//                    batch.foreachPartition(items -> {
//
//                        // Xt is the first element of the batch Bi
//                        boolean isFirst = true;
//
//                        // Recalibration
//                        if(isFirst && items.hasNext()){
//                            isFirst = false;
//                            for (Map.Entry<Long, Long> entry : S.entrySet()) {
//                                Long x = entry.getKey();
//                                Long fe = entry.getValue();
//
//                                // Number of tails before head of an unbiased coin
//                                boolean isHead = false;
//                                Long tau = 0L;
//                                float prob;
//                                while(!isHead){
//                                    tau = tau+1;
//                                    prob = random.nextFloat();
//                                    if(prob > 0.5){
//                                        isHead = true;
//                                    }
//                                }
//
//                                if(fe-tau > 0){
//                                    fe = fe-tau;
//                                }else{
//                                    S.remove(entry.getKey());
//                                }
//                            }
//                        }
//
//                        Long item = Long.parseLong(items.next());
//                        float prob = random.nextFloat();
//
//                        if(S.containsKey(item)){
//                            S.compute(item, (k, count) -> count+1);
//                        }else if(prob <= 1/Math.pow(2, countBatch.doubleValue())){
//                            S.put(item , 1L);
//                        }
//                    });
                });

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT STATISTICS PHASE
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS


        // Print information for the EXACT ALGORITHM
        System.out.println("EXACT ALGORITHM");

        // The size of the data structure used to compute the true frequent items
        System.out.println("Number of items in the data structure = " + histogram.size());

        // The threshold frequency
        double thresholdFrequency = phi * streamLength[0];

        // The number of true frequent items
        int countTrue = 0;

        // True frequent items
        ArrayList<Long> items = new ArrayList<>();

        for (Long key : histogram.keySet()) {
            if (histogram.get(key) >= thresholdFrequency) {
                countTrue++;
                items.add(key);
            }
        }
        System.out.println("Number of true frequent items = " + countTrue);

        // Sort the items
        Collections.sort(items);

        // Print the items
        for (Long it : items) {
                System.out.println(it);
        }


        // Print information for the RESERVOIR SAMPLING
        System.out.println("RESERVOIR SAMPLING");
        //The size m of the Reservoir sample
        System.out.println("Size m of the sample = " + m);

        //The number of estimated frequent items (i.e., distinct items in the sample)
        int countEstimateReservoir = 0;
        for (Long elem : reservoir) {
            if (histogram.get(elem) >= thresholdFrequency) {
                countEstimateReservoir++;
            }
        }
        System.out.println("Number of estimated frequent items = " + countEstimateReservoir);

        //The estimated frequent items, in increasing order (one item per line). Next to each item print a "+" if the item is a true freuent one, and "-" otherwise.
        Collections.sort(reservoir);

        System.out.println("Estimated frequent items:");
        for (Long elem : reservoir) {
            if (histogram.get(elem) >= thresholdFrequency) {
                System.out.println(elem + " +");
            }else{
                System.out.println(elem + " -");
            }
        }


        // Print information for the STICKY SAMPLING
        System.out.println("STICKY SAMPLING");

        //The size of the Hash Table
        System.out.println("Number of items in the Hash Table = " + S.size());

        //The number of estimated frequent items (i.e., the items considered frequent by Sticky sampling)
        int countEstimateSticky = 0;
        for (Long key : S.keySet()) {
            if (histogram.get(key) >= thresholdFrequency) {
                countEstimateSticky++;
            }
        }

        System.out.println("Number of estimated frequent items = " + countEstimateSticky);
        System.out.println("Estimated frequent items:");

        //The estimated frequent items, in increasing order (one item per line). Next to each item print a "+" if the item is a true freuent one, and "-" otherwise.
        for (Long key : S.keySet()) {
            if (histogram.get(key) >= thresholdFrequency) {
                System.out.println(key + " +");
            }else{
                System.out.println(key + " -");
            }
        }
    }
}
