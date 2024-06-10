import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class G039HW3 {
    // Sticky sampling lock to access the HashTable
//    static ReentrantLock stickyLock = new ReentrantLock();

    // The size m of the m-sample S (stored in reservoir Linked Queue)
    static ArrayList<Long> reservoir = new ArrayList<>();

    // Hash Table of the Sticky Sampling
    static HashMap<Long, Long> StickySampHashMap = new HashMap<>();

    // True frequency item counter
    static AtomicInteger processedTrueFreqItems = new AtomicInteger(0);

    // Processed item number (used in sticky sampling)
    static AtomicInteger processedItemNumReservoir = new AtomicInteger(0);

    // Processed item number (used in sticky sampling)
    static AtomicInteger processedItemNumSticky = new AtomicInteger(0);

    // Keep track of the previous bin in Sticky sampling to understand if an item is the first of a bin
    static final int[] prevBin = {-1};

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
        if (!(phi > 0 && phi < 1)) {
            throw new IllegalArgumentException("The argument phi must be in (0,1)");
        }
        if (!(epsilon > 0 && epsilon < 1)) {
            throw new IllegalArgumentException("The argument epsilon must be in (0,1)");
        }
        if (!(delta > 0 && delta < 1)) {
            throw new IllegalArgumentException("The argument delta must be in (0,1)");
        }
        if (!(portExp >= 8886 && portExp <= 8889)) {
            throw new IllegalArgumentException("The argument portExp must be in [8886, 8889]");
        }

        // SPARK SETUP
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName("Sampling");

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

        // r for the Sticky sampling
        double r = Math.log(1/(delta*phi))/epsilon;

        // Size m of the Reservoir Sampling
        int m = (int) Math.ceil(1/phi);

        // Hash Table for the true frequent elements
        HashMap<Long, Long> trueFreqElements = new HashMap<>();

        // Randomizer for generating probabilities
        Random random = new Random();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PROCESS PHASE (TRUE FREQUENCIES, RESERVOIR, EPSILON-AFI STICKY SAMPLING)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                .foreachRDD((batch, time) -> {
                    // Check whether the streaming has exceeded the limit
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        //1-The true frequent items with respect to the threshold phi

                        // Map obtained by counting how many times a given number appear in the stream batch
                        Map<Long, Long> batchCounts = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .filter((item) -> {
                                    if(processedTrueFreqItems.intValue() == n) {
                                        return false;
                                    }
                                    processedTrueFreqItems.incrementAndGet();
                                    return true;
                                })
                                .reduceByKey(Long::sum)
                                .collectAsMap();

                        // For each key appearing in my batch, add a new entry in the trueFreqElements hash map
                        // for counting how many instances of a number appear in the whole streaming
                        for (Map.Entry<Long, Long> pair : batchCounts.entrySet()) {
                            trueFreqElements.compute(pair.getKey(), (k, count) -> count == null ? pair.getValue() : count + pair.getValue());
                        }

                        // 2-An m-sample of Σ using Reservoir Sampling of, with m= ⌈1/phi⌉

                        // Given a batch iterate through every item of the batch
                        batch.foreach(item -> {
                            // If the number of processed items is equal to n
                            // do not process the remaining items in the batch
                            if(processedItemNumReservoir.get() == n) return;

                            // If the instant of time (processed item starting from index 1) is less than m, add the element to the m-sample
                            if (processedItemNumReservoir.get() + 1 <= m) {
                                reservoir.add(Long.parseLong(item));
                            } else {
                                // Probability of getting a substitution of an element in the m-sample with the current one
                                // N.B.: the probability decreases with time t (to keep uniform probability)
                                float probThreshold = (float) m / processedItemNumReservoir.get();

                                // Randomized probability
                                float prob = random.nextFloat();

                                // Check whether perform substitution and do it eventually
                                if (prob <= probThreshold) {
                                    // Substitute a random item of the m-sample
                                    reservoir.set(random.nextInt(reservoir.size()), Long.parseLong(item));
                                }
                            }
                            processedItemNumReservoir.incrementAndGet();
                        });

                        //3-The epsilon-Approximate Frequent Items computed using Sticky Sampling with confidence parameter delta
                        batch.foreach(item -> {
                            // If the number of processed items is equal to n
                            // do not process the remaining items in the batch
                            if(processedItemNumSticky.get() == n) return;

                            // Calculate the batch number of the processed item
                            // N.B. |B_0| = 2*r and |B_i| = (2^i)*r, i >= 1
                            int i = 0;

                            // Check whether the bin in which is contained the processed item is B_i with i >= 1
                            if (processedItemNumSticky.get() >= 2 * r) {
                                // The formula to calculate the bin index to which the item appertain is ⌊log_2(n/r)⌋,
                                // where n is the processed item number (processedItemNum)
                                // N.B. n ∈ [0, +∞)
                                i = (int) Math.floor(Math.log(processedItemNumSticky.get() / r) / Math.log(2));
                            }


                            // Recalibration

                            // If the item is the first of the batch
                            if (i != prevBin[0]) {
                                ArrayList<Long> keys;
                                keys = new ArrayList<>(StickySampHashMap.keySet());

                                for (long key : keys) {
                                    // Number of tails before head of an unbiased coin
                                    boolean isHead = false;
                                    int tau = 0;
                                    while (!isHead) {
                                        tau++;
                                        if (random.nextInt(2) == 0) {
                                            isHead = true;
                                            tau--;
                                        }
                                    }

                                    if (StickySampHashMap.get(key) - tau > 0) {
                                        StickySampHashMap.put(key, StickySampHashMap.get(key) - tau);
                                    } else {
                                        StickySampHashMap.remove(key);
                                    }

                                }
                            }

                            long tempItem = Long.parseLong(item);
                            float prob = random.nextFloat();


                            if (StickySampHashMap.containsKey(tempItem)) {
                                StickySampHashMap.compute(tempItem, (k, count) -> count + 1);
                            } else if (prob <= (1 / Math.pow(2, i))) {
                                StickySampHashMap.put(tempItem, 1L);
                            }


                            // Update the prevBin
                            prevBin[0] = i;

                            // Update the processed item count
                            processedItemNumSticky.incrementAndGet();
                        });

                        // If the stream has exceeded the limit, release the semaphore
                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });


        // MANAGING STREAMING SPARK CONTEXT
        sc.start();
        stoppingSemaphore.acquire();

        // This print is to understand how many items were processed
//        System.out.printf("Total retrieved items: %d\nProcessed True Frequent Item: %d\nProcessed Reservoir items: %d\nProcessed Sticky items: %d\n", streamLength[0], processedTrueFreqItems.get(), processedItemNumReservoir.get(), processedItemNumSticky.get());

        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // PRINT STATISTICS PHASE
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Print information for the EXACT ALGORITHM
        System.out.println("EXACT ALGORITHM");

        // The size of the data structure used to compute the true frequent items
        System.out.println("Number of items in the data structure = " + trueFreqElements.size());

        // The threshold frequency
        double thresholdFrequency = phi * processedTrueFreqItems.get();

        // The number of true frequent items
        int countTrue = 0;

        // True frequent items
        ArrayList<Long> trueFreqItems = new ArrayList<>();

        for (Long key : trueFreqElements.keySet()) {
            // Save the true frequent items in the data structure trueFreqItems according to the threshold and increment the counter
            if (trueFreqElements.get(key) >= thresholdFrequency) {
                countTrue++;
                trueFreqItems.add(key);
            }
        }
        System.out.println("Number of true frequent items = " + countTrue);

        // Sort the items
        Collections.sort(trueFreqItems);

        // Print the items
        for (Long it : trueFreqItems) {
            System.out.println(it);
        }


        // Print information for the RESERVOIR SAMPLING
        System.out.println("RESERVOIR SAMPLING");

        // The size m of the Reservoir sample
        System.out.println("Size m of the sample = " + m);

        // The number of estimated frequent items (i.e., distinct items in the sample)
        int countEstimateReservoir = 0;

        // The estimated frequent items (i.e., distinct items in the sample)
        ArrayList<Long> reservoirEstFreqItems = new ArrayList<>();

        for (Long elem : reservoir) {
            // Count only distinct items
            if (!reservoirEstFreqItems.contains(elem)) {
                countEstimateReservoir++;
                reservoirEstFreqItems.add(elem);
            }
        }
        System.out.println("Number of estimated frequent items = " + countEstimateReservoir);

        // Sort the items
        Collections.sort(reservoirEstFreqItems);

        // The estimated frequent items, in increasing order (one item per line).
        // Next to each item print a "+" if the item is a true frequent one, and "-" otherwise.
        System.out.println("Estimated frequent items:");
        for (Long elem : reservoirEstFreqItems) {
            if (trueFreqItems.contains(elem)) {
                System.out.println(elem + " +");
            } else {
                System.out.println(elem + " -");
            }
        }


        // Print information for the STICKY SAMPLING
        System.out.println("STICKY SAMPLING");

        // The size of the Hash Table
        System.out.println("Number of items in the Hash Table = " + StickySampHashMap.size());

        // The estimated frequent items
        ArrayList<Long> stickyEstFreqItems = new ArrayList<>();

        // The number of estimated frequent items (i.e., the items considered frequent by Sticky sampling)
        int countEstimateSticky = 0;
        for (Map.Entry<Long, Long> entry : StickySampHashMap.entrySet()) {
            if (entry.getValue() >= (phi-epsilon)*processedItemNumSticky.get()) {
                countEstimateSticky++;
                stickyEstFreqItems.add(entry.getKey());
            }
        }

        System.out.println("Number of estimated frequent items = " + countEstimateSticky);
        System.out.println("Estimated frequent items:");

        // Sort the array of estimated frequent items
        Collections.sort(stickyEstFreqItems);

        // The estimated frequent items, in increasing order (one item per line).
        // Next to each item print a "+" if the item is a true frequent one, and "-" otherwise.
        for (long item : stickyEstFreqItems) {
            if (trueFreqItems.contains(item)) {
                System.out.println(item + " +");
            } else {
                System.out.println(item + " -");
            }
        }
    }
}
