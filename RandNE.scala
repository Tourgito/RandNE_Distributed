import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.linalg.{Vector,Vectors,DenseMatrix, SparseMatrix,SparseVector, DenseVector}
import org.apache.spark.mllib.linalg.distributed.{DistributedMatrix,RowMatrix,IndexedRowMatrix,IndexedRow}
import scala.math.sqrt
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.rdd.RDD



 case class RandNE(spark: SparkSession, pathToEdgeList:String, graphsNumberOfNodes:String, dimensionality: Int, q: Int,
                   weights:List[Double], directedEdgeList: Boolean, sequencedNodesIds: Boolean,
                   edgeListSeperator:String,  numberOfPartitions: Int) {
  

    private val numberOfNodes: Int = this.graphsNumberOfNodes.toInt
   
    private val broadcastedWeights:Broadcast[List[Double]] = this.spark.sparkContext.broadcast(this.weights)

    // The adjacency matrix that preserves the network
    private val adjacencyMatrix:Broadcast[Array[(String, SparseVector)]] = this.createAdjacencyMatrix

    // The final matrix that contains the node emmbendings
    private var nodeEmbeddings:RDD[IndexedRow] = null

    

    // Implements Algorithm 1 in https://arxiv.org/pdf/1805.02396.pdf
    def execute: Unit = {
      
      // A List that contains the transpose matrices of the {U_0, U_1,.....,U_q} matrices 
      // At initialization, the transpose U_0 (Gaussian random matrix) is stored
      // Implements line 1 of Algorithm 1
      var U_list:List[RDD[(String,Array[Double])]] = List(RandomRDDs.normalVectorRDD(spark.sparkContext,this.dimensionality,this.numberOfNodes)
                                                                    .zipWithIndex()
                                                                    .map(x => (x._2.toString, x._1.toArray.map(y => ((1/sqrt(this.dimensionality)) * y.toDouble) * this.broadcastedWeights.value(0)))
                                                                        )
                                                         )
                                              
      // Implements lines 3-5 of Algorithm 1
      for (i <- 1 to this.q){
          U_list = U_list.:+(this.matricesMultiplication(U_list(i-1),i+1)) 
        }

      this.nodeEmbeddings = this.calculateEmbenddings(U_list)
      
      }





    // Multiply 2 Matrices: The adjacency matrix (IndexedRowMatrix) and a transpose U_i Matrix (DenseMatrix) )
    // implements the Matrices multiplication in line 4 in https://arxiv.org/pdf/1805.02396.pdf
    private def matricesMultiplication(U:RDD[(String, Array[Double])], matrixIndex:Int): RDD[(String,Array[Double])] = {
      
      U.map(x => {
                  val vector:Array[Double] = this.adjacencyMatrix.value.map(y => (new DenseMatrix(1, this.numberOfNodes, x._2).multiply(y._2).values(0) / this.broadcastedWeights.value(matrixIndex-2)) * this.broadcastedWeights.value(matrixIndex-1))
                  (x._1,vector)
                 }
           )

    }






    // Methods that impelements line 6 of Algorithm 1 in https://arxiv.org/pdf/1805.02396.pdf
    private def calculateEmbenddings(U_list:List[RDD[(String,Array[Double])]]): RDD[IndexedRow] = {

      val embenddings:RDD[IndexedRow] = U_list.reduce((x:RDD[(String,Array[Double])],y:RDD[(String,Array[Double])]) => {
                                                                                                                        x.union(y)
                                                                                                                        .reduceByKey((x:Array[Double],y:Array[Double]) => x.zip(y).map(r => r._1 + r._2))
                                                                                                                       }
                                                      )
                                               .map(x => new IndexedRow(x._1.toLong, new DenseVector(x._2)))

      new IndexedRowMatrix(embenddings).toCoordinateMatrix
                                       .transpose
                                       .toIndexedRowMatrix
                                       .rows                             

    }

    
    



    // Method that calculates the adjacency matrix that preserves the network. Since the network is undirected A = A transpose.
    private def createAdjacencyMatrix: Broadcast[Array[(String,SparseVector)]]  = {
        val adjacencyMatrix:Array[(String,SparseVector)] = this.spark.sparkContext.textFile(this.pathToEdgeList,this.numberOfPartitions)
                                                                                  .flatMap(edge => { 
                                                                                                    val nodesOfEdge: Array[String] = edge.split("\t") 
                                                                                                    if (this.directedEdgeList) // edge list is directed
                                                                                                      Array((nodesOfEdge(0), nodesOfEdge(1)))
                                                                                                    else   // edge list is undirected
                                                                                                      Array((nodesOfEdge(0), nodesOfEdge(1)), (nodesOfEdge(1), nodesOfEdge(0)))
                                                                                                    }
                                                                                          )
                                                                                  .groupByKey()
                                                                                  .zipWithIndex()
                                                                                  .flatMap(x => {
                                                                                                val nodeId: String = x._2.toString
                                                                                                val nodeNeighboors: Array[(String,String)]= x._1._2.map(y => (y, nodeId)).toArray :+ (x._1._1, (-nodeId.toInt).toString) 
                                                                                                nodeNeighboors
                                                                                                }
                                                                                          )
                                                                                  .groupByKey()
                                                                                  .flatMap(x=> {
                                                                                               val nodeId: String = (- x._2.toArray.sortBy(y => y.toInt).head.toInt).toString 
                                                                                               val nodeNeighboors = x._2.toArray.sortBy(y => y.toInt).drop(1).map(y => (y,nodeId)) // Gia nodeSketch den kanw drop
                                                                                               nodeNeighboors 
                                                                                               }
                                                                                          )
                                                                                  .groupByKey()
                                                                                  .map(x => {
                                                                                            val nodeVectorSize = x._2.toArray.length
                                                                                            val nodeNeighboorsIndices = x._2.toArray.map(y => y.toInt).sortBy(y => y)
                                                                                           (x._1,new SparseVector(this.numberOfNodes, nodeNeighboorsIndices, nodeNeighboorsIndices.map(y => 1.0)))
                                                                                            }
                                                                                      )
                                                                                   .sortBy(x => x._1)
                                                                                   .collect()

        this.spark.sparkContext.broadcast(adjacencyMatrix)
  }

 }
