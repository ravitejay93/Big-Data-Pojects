Steps to run code:

1) Unzip the assignment zip file. Import as maven build.
2) Run the project as maven build to generate the jar file : wordpartition-0.0.1-SNAPSHOT.jar
3) Get the JAR file from the target folder of the project.
4) Get the input file from the zip folder. The input files contains all the positive and negative words.
5) place the files inside the input folder at location : /user/rxy160030/input/ 
	-hdfs dfs -mkdir input
	-hdfs dfs -copyFromLocal positive-words.txt input/positive-words.txt
	-hdfs dfs -copyFromLocal negative-words.txt input/negative-words.txt
5) Move JAR file to the Home directory of the cluster.
6) Run the jar file using the following command:(the destination folder should not be present in the cluster.)
	hadoop jar wordpartition-0.0.1-SNAPSHOT.jar wordpartition.wordpartition.wordseg <destination>
	hadoop jar wordpartition-0.0.1-SNAPSHOT.jar wordpartition.wordpartition.wordseg /user/rxy160030/<output name>
	Example:
	hadoop jar wordpartition-0.0.1-SNAPSHOT.jar wordpartition.wordpartition.wordseg /user/rxy160030/out

7) The code takes the input files from the directory created in assignment1 (/user/rxy160030/assignemnt1). This folder should
   contain the 6 unzipped wikipedia text file from part-I (The folder should not contain any other files other than the 6 text file).
