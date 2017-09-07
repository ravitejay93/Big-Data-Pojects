1)Upload the code files from the directory codes into /user/<UserId>/
Example: /user/rxy160030/

2)Upload the data file from dataset directory into the cluster at /user/rxy160030/ 
		(Or) 
Upload the data file from dataset directory to other location /user/<UserId>/ and change the location in each file to /user/<UserId>/CA-HepTh.txt


3)Run each file using the following command:

spark-shell -i <FILE>
Example:
spark-shell -i question_a.scala
