package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class Main {
    static long startTime;
    static long endTime;
    static double executionTime;
    static boolean avgCheck = false;

    public static void main(String[] args) {

        long totalTimeStart = System.nanoTime();

        SparkSession spark = SparkSession.builder()
                .appName("project")
                .master("local")
                .getOrCreate();

        spark.conf().set("spark.sql.repl.eagerEval.enabled", "true");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        sc.setLogLevel("ERROR");

        //startTime();
        JavaRDD<String> rddBooks = spark.sparkContext().textFile("src/main/resources/books_cleaned.csv", 12).toJavaRDD();
        //analyzeBooks(rddBooks);
        //showEndTime();

        //startTime();
        JavaRDD<String> rddMovies = spark.sparkContext().textFile("src/main/resources/movies_cleaned.csv", 1).toJavaRDD();
        //analyzeMovies(rddMovies);
        //showEndTime();

        //getAvgTime(50, "book", rddBooks);

        getAvgTime(50, "movie", rddMovies);

        long totalTimeEnd = System.nanoTime();
        double totalExecutionTime = (totalTimeEnd - totalTimeStart) / 1e9;
        System.out.println("\n********************************************************************************************");
        System.out.println("              Total execution time: " + totalExecutionTime + " seconds.");
        System.out.println("********************************************************************************************\n");

        sc.stop();
    }

    public static void getAvgTime(Integer runs, String moviesOrBooks, JavaRDD<String> rdd) {
        avgCheck = true;

        long avgStartTime = System.nanoTime();

        if (Objects.equals(moviesOrBooks, "movie")) {
            for (int n = 1; n <= runs; n++) {

                if (n == runs) {
                    avgCheck = false;
                }

                startTime();
                analyzeMovies(rdd);
                showEndTime();
            }
        } else if (Objects.equals(moviesOrBooks, "book")) {
            for (int n = 1; n <= runs; n++) {

                if (n == runs) {
                    avgCheck = false;
                }

                startTime();
                analyzeBooks(rdd);
                showEndTime();
            }
        }

        endTime = System.nanoTime();
        executionTime = (endTime - avgStartTime) / 1e9;
        System.out.println("\n**************************************************************************************");
        System.out.println("Average execution time over " + runs + " runs: " + executionTime/runs + " seconds.");
        System.out.println("**************************************************************************************\n");

    }

    public static void analyzeBooks(JavaRDD<String> books) {

        String header = books.first().trim();
        JavaRDD<String> rddbooks = books.filter(line -> !line.trim().equals(header));

        //System.out.println("First element: " + books.first());

        startTime = System.nanoTime();

        // format the data into an RDD
        JavaRDD<Review> book_reviews = rddbooks.mapPartitions(book -> {
            List<Review> reviews = new ArrayList<>();
            while (book.hasNext()) {
                String[] parts = book.next().split(",");
                if (parts.length != 6) continue;
                String media_id = parts[0];
                String title = parts[1];
                String review_id = parts[2];
                double reviewScore = Double.parseDouble(parts[3]);
                String date = parts[4];
                ArrayList<String> genres = new ArrayList<>();
                genres.add(parts[5]);

                reviews.add(new Review(media_id, title, review_id, reviewScore, date, genres));
            }
            return reviews.iterator();
        });

        getHighestGenre(book_reviews, "Books");
    }

    public static void analyzeMovies(JavaRDD<String> movies) {

        String moviesheader = movies.first().trim();
        JavaRDD<String> rddMovies = movies.filter(line -> !line.trim().equals(moviesheader));

        startTime();

        JavaRDD<Review> movie_reviews = rddMovies.mapPartitions(movie -> {

            // format data into an RDD
            List<Review> reviews = new ArrayList<>();
            while (movie.hasNext()) {
                String[] parts = movie.next().split(",");
                //if (parts.length > 5) continue;

                String media_id = parts[0];
                String review_id = parts[1];
                String date = parts[2];
                double reviewScore = 0.0;
                try { // handle if review score is 0
                    if (!parts[3].isEmpty()) {
                        reviewScore = Double.parseDouble(parts[3]);
                    }
                } catch (NumberFormatException e) {
                    continue;
                }
                String title = parts[4];

                // in the movie dataset the genres have multiple entries
                // this code combines all the entries into one using array lists
                ArrayList<String> genres = new ArrayList<>();
                genres.add(parts[5]);

                if (parts.length > 6) {
                    int n = 1;
                    while (5+n < parts.length) {
                        genres.add(parts[5+n]);
                        n++;
                    }
                }
                reviews.add(new Review(media_id, title, review_id, reviewScore, date, genres));
            }
            return reviews.iterator();
        });

        getHighestGenre(movie_reviews, "Movies");
    }

    public static void getHighestGenre(JavaRDD<Review> reviews, String moviesOrBooks) {

        // this is the mapping part, it creates JavaPairRDDs in the format (genre, (score, review count))
        JavaPairRDD<String, Tuple2<Double, Integer>> ratingsAndCounts = reviews.mapToPair(review ->
                new Tuple2<>(review.getGenre(), new Tuple2<>(review.getReviewScore(), 1))
        );

        // this is where the data is reduced by key which is genre
        JavaPairRDD<String, Tuple2<Double, Integer>> avgRatingsAndCount = ratingsAndCounts.combineByKey(
                value -> value,
                (acc, value) -> new Tuple2<>(acc._1() + value._1(), acc._2() + value._2()),
                (acc1, acc2) -> new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2())
        );

        // get the average ratings
        JavaPairRDD<String, Double> avgRatings = avgRatingsAndCount.mapValues(value ->
                value._1() / value._2()
        );

        JavaPairRDD<String, Integer> reviewCounts = avgRatingsAndCount.mapValues(Tuple2::_2);

        JavaPairRDD<String, Tuple2<Double, Integer>> avgRatingsWithCount = avgRatings.join(reviewCounts);

        // this sorts by number of ratings in descending order, so highest number of ratings first
        List<Map.Entry<String, Tuple2<Double, Integer>>> ratingCountSorted = avgRatingsWithCount.collectAsMap().entrySet().stream()
                .sorted((entry1, entry2) -> Integer.compare(entry2.getValue()._2(), entry1.getValue()._2()))
                .limit(50)
                .toList();

        // the previous set is sorted again but this time by rating scores in descending order
        List<Map.Entry<String, Tuple2<Double, Integer>>> sorted = ratingCountSorted.stream()
                .sorted((entry1, entry2) -> Double.compare(entry2.getValue()._1(), entry1.getValue()._1()))
                .limit(10)
                .toList();

        // print results
        if (!avgCheck) {
            System.out.println("\n*****************************************************************************");
            System.out.println("Top 10 " + moviesOrBooks + " Genres:");
            for (Map.Entry<String, Tuple2<Double, Integer>> entry : sorted) {
                String genre = entry.getKey();
                double avgRating = entry.getValue()._1();
                int count = entry.getValue()._2();
                System.out.println(genre + " : Average Rating = " + Math.round(avgRating) + ", Number of Ratings = " + count);
            }
        }
    }

    public static void startTime(){
        startTime = System.nanoTime();
    }

    public static void showEndTime(){
        endTime = System.nanoTime();
        executionTime = (endTime - startTime) / 1e9;
        System.out.println("\nExecution time: " + executionTime + " seconds");
        System.out.println("*****************************************************************************");
    }

    static class Review implements Serializable {
        private final double reviewScore;
        private final String genre;

        public Review(String media_id, String title, String review_id, double reviewScore, String reviewDate, ArrayList<String> genre) {
            this.reviewScore = reviewScore;
            this.genre = String.valueOf(genre);
        }

        public String getGenre() {
            return genre;
        }

        public double getReviewScore() {
            return reviewScore;
        }
    }
}
