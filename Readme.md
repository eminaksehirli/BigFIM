# Frequent Itemset Mining for BigData

**DistEclat** and **BigFIM** are two Frequent Itemset Mining (FIM) algorithms 
that are implemented for the Hadoop platform. **DistEclat** is a distributed Eclat 
algorithm and **BigFIM** is a hybrid of the Apriori and Eclat algorithms. The 
details of the algorithms can be found in the paper [Frequent Itemset Mining for BigData][paper].

Given a list of transactions, FIM algorithms find frequently co-occuring 
items. A set of items is called frequent if they occur together in the 
transactions more than a user given threshold, called the _minimum support_ 
or _minSup_ for short. 

## DistEclat and BigFIM

**DistEclat** is a distributed version of Eclat. It has the advantages and 
disadvantages of the original algorithm: it is fast and mines long frequent
itemsets without combinatorial pattern explosion. The assumption of the 
original Eclat algorithm is that the transaction database fits into memory.
Therefore, **DistEclat** is the faster option if the database fits into memory.

For very large databases, the assumption of Eclat does not hold. Therefore, **BigFIM**
employs the Apriori algorithm to find relatively short frequent itemsets until their list 
of transactions fit into the memory. Then, it switches back to the Eclat algorithm
to distribute the search space and to complete frequent itemset mining.

If your database is small and you need Hadoop just for a speed up, you can 
use **DistEclat** for fast mining. If your data is larger than available 
memory of the nodes, then **BigFIM** is your only option. Mind that, **BigFIM** 
can be used for databases with any size.

Given that FIM produces more data than the original databases, if the 
data is too large, _sampling_ can be a better option.

## Downloading and building the code

The code can be found on [GitLab][gitlab]. It can be downloadable as a zip archive
following the link, or cloned as a `git` repository by the following command:

    git clone https://gitlab.com/adrem/bigfim-sa.git

Our project uses [maven][maven] for build system. Following command should 
download the dependencies and create the required `jar` file in `target` directory. 

    mvn package

Note that, `jar` file includes all the dependencies, and hence, it is rather large.


## A Running Example

Here we will mine frequent itemsets using FIM algorithms, step by step.

We will use the retail dataset, which can be downloaded from [here][retail-link].
More sample datasets can be found on [fimi repository][fimi]. 

If you haven't already started the HDFS and Hadoop, you can do so by executing the
following commands. **WARNING:** The following commands will delete everything 
on HDFS. Only use them if you are working on a stand-alone installation which 
is not used as a production environment and there is absolutely no important data 
on HDFS. If you only want to start the cluster, use only the second command.

    $HADOOP_PREFIX/bin/hadoop namenode -format
    $HADOOP_PREFIX/bin/start-all.sh

Change your directory to your BigFIM folder:

    cd ~/path/to/your/BigFIM

If you haven't done already, you can download the [retail.dat][retail-link] using
the following command:

    wget http://fimi.ua.ac.be/data/retail.dat
    
Put the datafiles into HDFS:

    $HADOOP_PREFIX/bin/hadoop fs -mkdir input
    $HADOOP_PREFIX/bin/hadoop fs -put retail.dat input

Run DistEclat miner:

    $HADOOP_PREFIX/bin/hadoop jar target/bigfim-sa-1.0-jar-with-dependencies.jar \
        be.uantwerpen.adrem.disteclat.DistEclatDriver \
        -i input/retail.dat \
    	-o output \
    	-s 60 \
    	-m 2 \
    	-p 3

It will run for a while. Let's look through the parameters:

- `-i input-file`: Mine the transaction db `input-file`. It should be already in
  your home directory on HDFS.
- `-o output-dir`: Frequent itemsets and in-between files will be created 
  in this folder. If the directory exists, it will be deleted first.
- `-s 60`: Minimum support threshold for the frequent itemsets.
- `-m 2`: Number of mappers that is going to be used. It is recommended to 
  select this as the number of available machines.
- `-p 3`: The depth of the prefix tree (length of the prefixes) to mine before 
  distributing the search space further. 

When the mining finishes, get the frequent itemsets out of HDFS:

    $HADOOP_PREFIX/bin/hadoop fs -get output . 
   
The file `output/fis/part-r-00000` lists the frequent itemsets.
Please note that the file is encoded for compression. To decode the file and 
write all the individual frequent itemsets to `/tmp/out.txt` use the following 
command:

    $HADOOP_PREFIX/bin/hadoop jar target/bigfim-sa-1.0-jar-with-dependencies.jar \
        be.uantwerpen.adrem.eclat.util.TrieDumper \
    	output/fis/part-r-00000 \
    	/tmp/out.txt
 
For the format of the output please refer to the project [wiki][wiki].
















[gitlab]:https://gitlab.com/adrem/bigfim-sa/
[paper]:http://adrem.uantwerpen.be/sites/adrem.uantwerpen.be/files/SD223_Frequent_Itemset_Mining_for_Big_Data.pdf
[fimi]:http://fimi.ua.ac.be/data/
[wiki]:https://gitlab.com/adrem/bigfim/wikis/home
[maven]:https://maven.apache.org/ 
[retail-link]:http://fimi.ua.ac.be/data/retail.dat
