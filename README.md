# RUNNING CODE FROM CLUSTER:

## 1) IMPORT JAR
Copy Jar to cluster



## 2) RUN GET COUNTS
To run GetArticlesMapred (articles to people articles), use:

	<HADOOP-PATH> jar <JAR PATH> Train/GetCountsMapred <INPUT-PATH> <OUTPUT-PATH>
	
i.e.:
	
	bin/hadoop jar NaiveBayes.jar code/articles/GetArticlesMapred output/lemmas output/counts

	
	
## 3) RUN GET PROBS
To run LemmaIndexMapred (people articles to lemma count indexes), use:

	<HADOOP-PATH> jar <JAR PATH> train/GetProbsMapred <INPUT-PATH> <OUTPUT-PATH>
	
i.e.:

	bin/hadoop jar NaiveBayes.jar train/GetProbsMapred output/counts output/probs

	
	
## 4) RUN CLASSIFICATION
To run InvertedIndexMapred  (lemma count indexes to inverted index), use:

	<HADOOP-PATH> jar <JAR PATH> classify/GetArticlesMapred <INPUT-PATH> <OUTPUT-PATH>
	
i.e.:

	bin/hadoop jar NaiveBayes.jar classify/InvertedIndexMapred output/lemmas output/predictions




	
# HADOOP COMMANDS:

## CHECK CURRENT JOB STATUS
if you want to check the status of a job after losing a connection or killing the automatic report:

	<HADOOP-PATH> job -status <JOB-ID>

i.e.:

	bin/hadoop job -status job_1409756850487_0152


## CHECK CURRENT JOBS IN OUR QUEUE
if you don't have a job id, here's how you can find it:

	<HADOOP-PATH> queue -info <OUR-GROUP-NAME> -showJobs

i.e.

	bin/hadoop queue -info hadoop14 -showJobs


## CHECK WHO ELSE IS RUNNING JOBS
if you want to see how many other people are running jobs at the same time:

	<HADOOP-PATH> queue -list

i.e.	

	bin/hadoop queue -list
