# Local Instructions
1. copy files to a directory: `git clone https://gist.github.com/cc7c8cec1188fd387cc2e3ec0f4fed7a.git wordcount` and then `cd wordcount`.
2. see the input files: `cat *.txt`
3. make sure mapper and reducer are executable `chmod +x *.scala`
4. see how mapper works: `cat baa.txt | ./mapper.scala`
5. see how reducer works: `cat baa.txt | ./mapper.scala | ./reducer.scala`

# Hadoop Instruction
1. copy files to a directory: `git clone https://gist.github.com/cc7c8cec1188fd387cc2e3ec0f4fed7a.git wordcount` and then `cd wordcount`.
2. create a directory on HDFS: `hadoop fs -mkdir -p /wc/in`
3. copy input files into HDFS: `hadoop fs -put *.txt /wc/in`
4. make sure the files are transfered: `hadoop fs -ls /wc/in` You can also read their content using `-cat`
5. make sure the mapper and reducer scripts are executable: `chmod +x *.scala`
6. make sure the output directory dose NOT exist: `hadoop fs -ls /wc/out`
7. issue:  `hadoop jar /home/user/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar  
-mapper mapper.scala 
-reducer reducer.scala 
-input /wc/in/* 
-output /wc/out`
8. make sure the above script run successfully: `hadoop fs -ls /wc/out` You should see a zero byte file called `_SUCCESS`
9. read the output: `hadoop fs -cat /wc/out/part-00000`
