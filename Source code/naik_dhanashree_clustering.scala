import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.PriorityQueue
import java.io._

object naik_dhanashree_clustering {
    
    
	def createDataMap(data:String): Map[(Float,Float,Float,Float),String] = {
		
		 val array = data.split(",")
		 //Creating the Map of values
		 val dataMap = Map[(Float,Float,Float,Float),String](
		 ( (array(0).toFloat,array(1).toFloat,array(2).toFloat,array(3).toFloat) -> array(4).toString() ) 
		 )
		return dataMap
		}
	
	def pairwise_distance(data: Array[(Float, Float, Float, Float)]) : scala.collection.mutable.Map[((Float, Float, Float, Float), (Float, Float, Float, Float)),Float] = {
     
     var datamap =  scala.collection.mutable.Map[((Float, Float, Float, Float), (Float, Float, Float, Float)),Float]()
     
     for(i <- 0 to (data.size - 2) ){
       for(j <- i+1 to data.size-1){
         
         val diff = math.sqrt(math.pow((data(i)._1 - data(j)._1 ),2).toFloat + math.pow((data(i)._2 - data(j)._2 ),2).toFloat + math.pow((data(i)._3 - data(j)._3 ),2).toFloat + math.pow((data(i)._4 - data(j)._4 ),2).toFloat).toFloat
         datamap +=  ((data(i),data(j)) -> diff)
         
       }
     }
    return datamap
    }
	
   def find_dist(i:(Float, Float, Float, Float),j:(Float, Float, Float, Float)) : Float ={
     
       val diff = math.sqrt(math.pow((i._1 - j._1 ),2).toFloat + math.pow((i._2 - j._2 ),2).toFloat + math.pow((i._3 - j._3 ),2).toFloat + math.pow((i._4 - j._4 ),2).toFloat).toFloat
       return diff
     
   }
   def creatMap3(points: Array[(Float, Float, Float, Float)]) : scala.collection.mutable.Map[(Float, Float, Float, Float),scala.collection.Set[(Float, Float, Float, Float)]] = {
     var datamap =  scala.collection.mutable.Map[(Float, Float, Float, Float),scala.collection.Set[(Float, Float, Float, Float)]]()
     for (i<- points){
       
       datamap += i ->scala.collection.Set(i)
     }
     return datamap
   } 
	
   def main(args: Array[String]) {
     
	   val number_of_clusters = args(1).toInt
	   val datafile = args(0)
	   val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]")
	   val sc = new SparkContext(conf)
	   val dataFileLines = sc.textFile(datafile , 2).cache()
	   val dataMap = dataFileLines.flatMap(f=>createDataMap(f))
	   val new_dataMap = dataMap.groupByKey 
	   //new_dataMap.foreach(f=>println(f))
	   val dups = scala.collection.mutable.ListBuffer[Set[(Float,Float,Float,Float)]]()
	   val Mapped_dups = scala.collection.mutable.Map[Set[(Float,Float,Float,Float)],Int]()
	   val output = new PrintWriter(new File("dhanashree_naik_cluster_"+number_of_clusters+".txt" ))
	   var xx = new_dataMap.collect
	   xx.foreach(f=> 
	     if (f._2 .size >1){
	      
	       dups += Set(f._1)
	       Mapped_dups += ( Set(f._1) -> (f._2 .size-1))
	       
	     })
	   //println(dups)
	   
	   
	   var points = scala.collection.mutable.Set(new_dataMap.keys.collect:_*)	   
	   var clusters = creatMap3(new_dataMap.keys.collect)  
       def d(t2: (((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)) = t2._2
       var Prio_Queue:PriorityQueue[(((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)]=PriorityQueue()(Ordering.by(d).reverse)   
	   val dataArr = new_dataMap.keys.collect  
	   
	   var lookup = pairwise_distance(dataArr)  
	   var diff = lookup.foreach(f=> Prio_Queue+=(((f._1 ._1 ,f._1 ._2) ,f._2) ))
       //println(lookup.size)
      
      var discard_set = scala.collection.mutable.Set[(Float,Float,Float,Float)]() 
	  var c=4
	   while (clusters.size > number_of_clusters)
	   	{ 
	     
	    
	    var new_cluster_set = scala.collection.Set[(Float,Float,Float,Float)]() 
	    var temp = Prio_Queue.dequeue._1
	    while ((discard_set.contains(temp._1 )) || (discard_set.contains(temp._2 ))){
	      temp = Prio_Queue.dequeue._1
	    }
	    var item_to_remove = Set(temp._1 ,temp._2 )
	    //val new_points = points.diff(item_to_remove)
	    //points =points.empty
	    for (s <- item_to_remove) discard_set += s
	   
	    
	    	   
	    val element1 =  clusters.getOrElse(temp._1 , Set[(Float,Float,Float,Float)]() )
	    val element2 =  clusters.getOrElse(temp._2 , Set[(Float,Float,Float,Float)]() )
	    	 
	      
	    new_cluster_set = element1.union(element2)
	    clusters.remove((temp._1))
	    clusters.remove(temp._2 )
	    
	    var sum1 = (0.toFloat)
	    var sum2 = 0.toFloat
	    var sum3 = 0.toFloat
	    var sum4 = 0.toFloat
	    for (i<- new_cluster_set){
	      sum1 = sum1 + i._1 
	      sum2 = sum2 + i._2 
	      sum3 = sum3 + i._3
	      sum4 = sum4 +i._4 
	    }
	    
	    
	    val new_centroid = ((sum1/new_cluster_set.size),(sum2/new_cluster_set.size),(sum3/new_cluster_set.size),(sum4/new_cluster_set.size))
	    
	    
	    
	    
	    for (i<-clusters.keys){
	      var dist = find_dist(new_centroid,i)
	      Prio_Queue += (((new_centroid,i),dist))
	    } 
	    
	    
	    clusters(new_centroid)=new_cluster_set
	    
	    
	    
	   	}
    
   var num_clusts = scala.collection.mutable.ListBuffer[List[(Float,Float,Float,Float)]]()	
   var clust = List[(Float,Float,Float,Float)]()
   
   for (f <- clusters){
     clust = f._2 .toList
     for(i<-Mapped_dups){
       if(f._2 .intersect(i._1 ) == i._1 ){
         for (j<-0 until i._2 ){
           for(k<-i._1 ){
             clust = k :: clust
           }
         }
       }
     }
     num_clusts += clust
     clust = List[(Float,Float,Float,Float)]()
    
   }
   var datamap_final = dataMap.collect.toMap
   var num_clusts_final = scala.collection.mutable.ListBuffer[List[((Float,Float,Float,Float),String)]]()	
   var clust_final = List[((Float,Float,Float,Float),String)]()
   for (i<- num_clusts){
     for (j <- i) {
       clust_final = (j,datamap_final(j)) :: clust_final
     }
     num_clusts_final += clust_final
     clust_final = List[((Float,Float,Float,Float),String)]()
   }
   var clust_count = 1
   var wrong_count = 0
   for (i<-num_clusts_final ){
     
     //println("cluster:"+ clust_count)
     var final_map = scala.collection.mutable.Map[(Float,Float,Float,Float),String]()
     for (j<-i){
       final_map += (j._1 -> j._2 )}
     var groupped = final_map.groupBy(f=>f._1 )
     var countings = List[Int]()
     var count_map = scala.collection.mutable.Map[Int,(Float,Float,Float,Float)]()
     groupped.foreach(f=>{

       count_map += (f._2.size -> f._1 );
       countings = f._2 .size :: countings
       
       
     }
       
     )
     output.write("cluster:" + datamap_final(count_map(countings.max))+"\n")
     
     for (j<-i){
       
       output.write("["+ j._1._1 +", "+j._1._2 +", "+j._1 ._3 +", " +j._1 ._4  + ", "+ j._2 .toString()+ "]"+"\n")
       if (j._2 != datamap_final(count_map(countings.max)) ){
         wrong_count +=1
       }
     }
     output.write("Number of points in this cluster:"+i.size+"\n")
     output.write("\n")
       
   }
   output.write("Number of points wrongly assigned:"+wrong_count)
   output.close()
   }
   
   
   
   
}