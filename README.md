# Distributed RandNE implementation

#### This repository provides a distributed implementation of the RandNE method in scala, which is developed utilizing the Apache Spark framework. Below, at first the case class that implements the RandNE method is presented. Then each of the RandNE case class parameters is analyzed. At last, the method of the RandNE case class that executs the RandNE method is analyzed. A detail analyzes of the implementation's logic provided by the repository is available in my postgraduate thesis (na balw link) in chapter 4. 

---

## RandNE case class

<pre>
 case class RandNE(spark: SparkSession,
                   pathToEdgeList:String,
                   graphsNumberOfNodes:String,
                   dimensionality: Int,
                   q: Int,
                   weights:List[Double],
                   directedEdgeList: Boolean,
                   edgeListSeperator:String,
                   numberOfPartitions: Int
                   )
</pre>



## Parameters

<pre>
<Strong>spark : <i> SparkSession </i></Strong>
                 The SparkSession object of the application.

<Strong>pathToEdgeList : <i> String </i></Strong>
                 The path of the edge list that contains the network
                 to which the RandNE will be executed.
                 
<Strong>graphsNumberOfNodes : <i> String </i></Strong>
                 The number of nodes of the network.
                 
<Strong>dimensionality : <i> Int </i></Strong>
                 The size of the node embeddings.
                 
<Strong>q : <i> Int </i></Strong>
                The proximity order
                
<Strong>weights : <i> List[Double] </i></Strong>
                 The predifined weights of the method.
                 
<Strong>directedEdgeList : <i> Boolean </i></Strong>
                 If the edge list is directed then true else false.

<Strong>EdgeListSeperator : <i> String </i></Strong>
                 The sumbol that seperates the 2 edges in each line of the edge list file.
                 
<Strong>NumberOfPartitions : <i> Int </i></Strong>
                 The number of partitions in which the RDDs are going to be partitioned. 
</pre>

## Methods

<pre>
<Strong>execute : <i>Unit</i></Strong>
                 Executes the RandNE method to produce the node embeddings. After the 
                 execution of the <i> execute </i> method, the node embeddings are preserved
                 in the <i> nodeEmbeddings </i> variable, which is of type RDD[IndexedRow].
</pre>
