Steps to run code:

1) Unzip the assignment zip file. Import as maven build.
2) Run the project as maven build to generate the jar file : wordpartition-0.0.1-SNAPSHOT.jar
3) Get the JAR file from the target folder of the project.
4) Get the input file from the zip folder. The input files contains all the parts of speech ( "mobyposi.i" ).
5) place the files inside the input folder at location : /user/rxy160030/input/ (make a directory if not present)
	-hdfs dfs -mkdir input
	-hdfs dfs -copyFromLocal mobyposi.i input/mobyposi.i

5) Move JAR file to the Home directory of the cluster.
6) Run the jar file using the following command:(the destination folder should not be present in the cluster.)
	hadoop jar pos-0.0.1-SNAPSHOT.jar partofspeech.pos.pos <destination>
	hadoop jar pos-0.0.1-SNAPSHOT.jar partofspeech.pos.pos /user/rxy160030/<output name>
	Example:
	hadoop jar pos-0.0.1-SNAPSHOT.jar partofspeech.pos.pos /user/rxy160030/out

7) The code takes the input files from the directory created in assignment1 (/user/rxy160030/assignemnt1). This folder should
   contain the unzipped wikipedia text files from part-II (The folder should not contain any other files other than the wiki text file).
