package com.unphon;

import static java.lang.Integer.compare;
import static java.lang.Integer.parseInt;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class WordCount {

    private static final String INPUT_FILE = "src/main/resources/tempest.txt";
    private static final int LIMIT = 10;

    public static void main(String[] args) throws Exception {

        System.out.println("\n\n*** Program that counts unique words from a text file and lists the top" +
            LIMIT + " occurrences***");
        doWordCount(INPUT_FILE, LIMIT);
    }

    private static void doWordCount(String fileName, int max) throws Exception {
        long start = Instant.now().toEpochMilli();
        ConcurrentHashMap<String, LongAdder> wordCounts = new ConcurrentHashMap<>();
        System.out.println(">>> Reading file: "+fileName);

        Path filePath = Paths.get(fileName);

        Files.readAllLines(filePath)
            .parallelStream()                               // Start streaming the lines
            .map(line -> line.split("\\s+"))                // Split line into individual words
            .flatMap(Arrays::stream)                        // Convert stream of String[] to stream of String
            .parallel()                                     // Convert to parallel stream
            .filter(w -> w.matches("\\w+"))                 // Filter out non-word items
            .map(String::toLowerCase)                       // Convert to lower case
            .forEach(word -> {                              // Use an AtomicAdder to tally word counts
                if (!wordCounts.containsKey(word)) {
                    // If a hashmap entry for the word doesn't exist yet
                    wordCounts.put(word, new LongAdder());  // Create a new LongAdder
                }
                wordCounts.get(word).increment();           // Increment the LongAdder for each instance of a word
            });

        wordCounts
            .keySet()
            .stream()
            .map(key -> String.format("%s ( %d )", key, wordCounts.get(key).intValue()))
            .sorted((prev, next) -> compare(parseInt(next.split("\\s+")[2]), parseInt(prev.split("\\s+")[2])))
            .limit(max)
            .forEach(t -> System.out.println("\t" + t));

        long end = Instant.now().toEpochMilli();
        System.out.println(String.format("*** Program completed in %d milliseconds ***", (end-start)));
    }
}
