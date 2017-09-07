Steps to run code:

1) Unzip the assignment zip file. Import as maven build.
2) Run the project as maven build to generate the jar file : parti-0.0.1-SNAPSHOT.jar
3) Get the JAR file from the target folder of the project.
4) Get the input file from the zip folder. The input file contains all the links to downloadable files.
5) Move the input file and JAR file to the Home directory of the cluster.
6) Run the jar file using the following command(the jar and input file should be in the same folder)
	hadoop jar parti-0.0.1-SNAPSHOT.jar parti.parti.DownloadDataLink <Input File> <location>
	hadoop jar parti-0.0.1-SNAPSHOT.jar parti.parti.DownloadDataLink input.txt hdfs://cshadoop1/user/<User ID>/
	Example:
	hadoop jar parti-0.0.1-SNAPSHOT.jar parti.parti.DownloadDataLink input.txt hdfs://cshadoop1/user/rxy160030/
