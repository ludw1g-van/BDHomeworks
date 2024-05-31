import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Float;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.Semaphore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
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

        // Atomic integer used to keep track of the instant of time (also between batches)
        AtomicInteger t = new AtomicInteger(0);

        // Randomizer for generating probabilities (used in Reservoir sampling)
        Random random = new Random();

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

                        // The size m of the m-sample S (stored in reservoir ArrayList)
                        int m = (int) Math.ceil(1 / phi);
                        ArrayList<Long> reservoir = new ArrayList<>(m);

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
        System.out.println("Number of items processed = " + streamLength[0]);
        System.out.println("Number of distinct items = " + histogram.size());

        // Find the largest item
        long max = 0L;
        for (Long key : histogram.keySet()) {
            if (key > max) {max = key;}
        }
        System.out.println("Largest item = " + max);

        // Print true frequent items
        double thresholdFrequency = phi * streamLength[0];
        for (Long key : histogram.keySet()) {
            if (histogram.get(key) >= thresholdFrequency) {
                System.out.println("frequency of item " + key.toString() + "=" + histogram.get(key));
            }
        }
    }
}

// GARBAGE COLLECTION
//3-The epsilon-Approximate Frequent Items computed using Sticky Sampling with confidence parameter delta
                        /*final Map<String, Integer> sample = new HashMap<>();
                        double r = Math.log(1 / (delta*phi)) / epsilon;  // Sample rate

                        // Create a DStream that connects to hostname:port
                        JavaPairDStream<String, Integer> lines = jssc.socketTextStream(hostname, port)
                                .mapToPair(s -> new Tuple2<>(s, 1))
                                .reduceByKey(Integer::sum);

                        batch.foreachPartition(iterator -> {
                            while (iterator.hasNext()) {
                                Tuple2<String, Integer> tuple = iterator.next();
                                String item = tuple._1();
                                int count = tuple._2();
                                int currentCount = sample.getOrDefault(item, 0);

                                if (currentCount > 0) {
                                    // If already in the sample, increment the count
                                    sample.put(item, currentCount + count);
                                } else {
                                    // Calculate current sampling rate
                                    double n = rdd.context().getTotalStorageMemory(); // This is a placeholder; calculate n properly
                                    double samplingRate = r / n;

                                    // Add new item with calculated probability
                                    if (random.nextDouble() < samplingRate) {
                                        sample.put(item, count);
                                    }
                                }
                            }
                        });

                        // Periodically print the sample
                        lines.foreachRDD(rdd -> {
                            System.out.println("Sampled items:");
                            sample.forEach((key, value) -> {
                                if (value > (phi - epsilon) * rdd.count()) {
                                    System.out.println("Item: " + key + ", Count: " + value);
                                }
                            });
                        });

                        // Extract the distinct items from the batch
                        Map<Long, Long> batchItems = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey((i1, i2) -> 1L)
                                .collectAsMap();
                        // Update the streaming state
                        for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                            if (!histogram.containsKey(pair.getKey())) {
                                histogram.put(pair.getKey(), 1L);
                            }
                        }*/