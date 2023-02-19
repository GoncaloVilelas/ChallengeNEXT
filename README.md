<h1 align="center"> Challenge NEXT </h1> <br>

<p align="center">
  This is an assessment project, the objective is to process a list of phone numbers taking into account several factors, including the country code.
</p>


## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Testing](#testing)




## Introduction

This application was made as a challenge for NEXT Engineering. It counts how many numbers from a specific country are in an input file passed as an argument when running the JAR. It was made during weekend and there is enough space for improvement, but I had no time for it. Despite that, I'm really happy with the solution that I came with.

## Features

The solution that I produced is based in using Spark library in Java. All files were extracted using a SparkSession and were converted to Dataframes. Firstly, I tried to handle data so it would be easier to cross data and count. Regarding Short numbers, my idea was to query the dataframe and filter all entries by size, by not having white spaces and not starting with a 0. Regarding Long numbers, it was a bit more sophisticated. The idea was to firstly filter all numbers by the first characters (not starting with + or 00  succeeded by a white space), remove all the white spaces present in the result and all characters that were not numeric and lastly filter by size. After this phase, I joined both dataframes conditionally, by the starting characters of each phone number matching or not one of the codes related to the countries. Then, I would just need to group the join dataframe by the coutry name and make a count (also sum the number of short numbers to the Portugal sum).

I created also two separate classes that each one represent the schema of the Input file and the schema of the countryCode.txt file. If I had more time I would also like to create a class for Short Numbers and Long Numbers at least. I decided to put the result in a csv file using Hadoop. The write method was creating multiple files, one by worker, but I decided to coalesce all files into one. This is not recommended when there are a lot of small files because performance would degrade a lot, but in this case I thought it would be nicer to see the result. I spotted one curious aspect in the algorithm: Canada and United States have the same code, so for phone numbers with that code I counted for both countries. The result is ordered in a descending order firstly by count number and then by Country name.


## Requirements
The application can be run locally, the requirements for each setup are listed below.


### Local
* [Java 8 SDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Maven](https://maven.apache.org/download.cgi)
* [Spark](https://spark.apache.org/downloads.html)
* [Hadoop](https://hadoop.apache.org/releases.html)


## Quick Start

### Run Local
First compile the app to create a JAR with all the dependencies inside. Must be inside sourcecode to run this command!
```bash
$ mvn compile package
```
Then, run the JAR that was created on target folder. The <PATH-TO-INPUT-FILE> is the path to the input file on local file system (ex: "C:\Users\Gonçalo Vilelas\Desktop\input.txt")
```bash
$ java -jar .\target\sourcecode-1.0.0-jar-with-dependencies.jar <PATH-TO-INPUT-FILE>
```
Before running the app, be sure that the folder "results.csv" is deleted inside src/main/java/com/challenge!
## Testing
TODO: Additional instructions for testing the application.