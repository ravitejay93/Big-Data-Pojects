Steps to run code:

1) Unzip the assignment zip file. Import as maven build.
2) Run the project as maven build to generate the jar file : lab4-0.0.1-SNAPSHOT.jar
3) Get the JAR file from the target folder of the project.
4) Move JAR file to the Home directory of the cluster.
6) 
	Q1)Run the jar file using the following command:(the destination folder should not be present in the cluster.)
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesscount <destination>
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesscount /user/rxy160030/<output name>
		Example:
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesscount /user/rxy160030/out
	
	Q2)Run the jar file using the following command:(the destination folder should not be present in the cluster.)
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businessinny <destination>
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businessinny /user/rxy160030/<output name>
		Example:
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businessinny /user/rxy160030/out
	
	Q3)Run the jar file using the following command:(the destination folder should not be present in the cluster.)
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesszipcode <destination>
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesszipcode /user/rxy160030/<output name>
		Example:
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.businesszipcode /user/rxy160030/out

	Q4)Run the jar file using the following command:(the destination folder should not be present in the cluster.)
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness <destination>
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness /user/rxy160030/<output name>
		Example:
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness /user/rxy160030/out

	Q5)Run the jar file using the following command:(the destination folder should not be present in the cluster.)
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness2 <destination>
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness2 /user/rxy160030/<output name>
		Example:
		hadoop jar lab4-0.0.1-SNAPSHOT.jar lab4.lab4.reviewbusiness2 /user/rxy160030/out

