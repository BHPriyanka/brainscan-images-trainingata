package parseTIFFImage

import org.apache.spark.sql.SparkSession
import java.io.File
import javax.imageio.ImageIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions.asScalaBuffer
import java.io._
import org.apache.commons.io._

/**
 * ClassificationModel: Scala application to parse the images stored as a byte array in the input files
 * 											and generate huge amount of training data
 * @author b h priyanka
 */
object ClassificationModel {
  /**
   * Initialize the X,Y and Z values of the dimensions of the neighbourhood to be calculated
   */
  var maxX=0;
	var maxY=0;
	var maxZ=0;
	
  def main(args : Array[String]) {
	  val spark = SparkSession.builder()
			  .appName("Neighbourhood Vectors application")
			  .master("local[*]")
			  .config("spark.sql.warehouse.dir", "file:///C:/Users/priya/workspace/Assignment5/park-warehouse")
			  .getOrCreate()

			  System.setProperty("hadoop.home.dir", "C:/winutils");

	  maxX = Integer.parseInt(args(1))
		maxY = Integer.parseInt(args(2))
		maxZ = Integer.parseInt(args(3))

		val sc = spark.sparkContext
				
		//sc.addJar("s3://pryanka-training-bucket/Assignment5-0.0.1-SNAPSHOT.jar")
		//sc.binaryFiles("s3://pryanka-training-bucket/LoadMultiStack.class")
		//sc.addFile("s3://pryanka-training-bucket/input/1_image.txt")
		//sc.addFile("s3://pryanka-training-bucket/input/1_dist.txt")
		
		//width=512 pixel
		//height=512 pixel
		//z = 33..60
	  val xDim = 100;
	  val yDim = 100;

	  /**
	   * Run the spark application for 5 images
	   */
	  for(imageNumber <- 1 to 1) {
	    /**
	     * Read the input file to a Array[Byte]
	     */
	      val t1 = System.nanoTime
	      //val bis = new BufferedInputStream(new FileInputStream(args(0)+imageNumber+"_image"))
				//var imageDetails = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
	      //var imageDetails = LoadMultiStack.parseImage("./input/1_image.tiff", xDim, yDim)
	      var str = Source.fromFile("./input/1_image.txt").getLines().mkString
	      var details = str.split("~~")
	      var imageDetails = details(0).split(",")
	      	        
        var imageList = new ListBuffer[String]()

        /**
         * Scan the imageBytes array and fetch the corresponding pixel's x,y,z coordinates and the brightness values
         */
				for (iz <- 0 to imageDetails.size/(xDim*yDim)-1) {
					for (iy <- 0 to yDim-1) {
						for (ix <- 0 to xDim-1) {
							val b = imageDetails(iz * yDim * xDim + iy * xDim + ix);
							imageList += ix.toString() + ";" + iy.toString() + ";"+ iz.toString() +";" + b.toString()
						}
					}
				}

			/**
       * Input: ListBuffer[String]
       * Output: RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), brightnessValue: Int]
       * Parses every line from the input List and creates an RDD with the x,y,z triplet as the key
       * and the brightness as the value
       */
		  var imagePixels = sc.parallelize(imageList.toList)
				                  .map(image => (((Integer.parseInt(image.split(";")(0)),Integer.parseInt(image.split(";")(1)),
				                      Integer.parseInt(image.split(";")(2))), Integer.parseInt(image.split(";")(3)))))

			/**
			 * 	Read *_dist.tiff files
			 */
			//val bis1 = new BufferedInputStream(new FileInputStream(args(1)+imageNumber+"_dist"))
			//var distDetails = Stream.continually(bis1.read).takeWhile(-1 !=).map(_.toByte).toArray
		  //var str1 = Source.fromFile("s3://pryanka-training-bucket/input/1_dist.txt").getLines().mkString
		  var distDetails = details(1).split(",")
		  //var distDetails = LoadMultiStack.parseImage(args(1), xDim, yDim)
			var distList = new ListBuffer[String]()

			for (iz <- 0 to distDetails.size/(xDim*yDim)-1) {
			  for (iy <- 0 to yDim-1) {
				  for (ix <- 0 to xDim-1) {
					  val b = distDetails(iz * yDim * xDim + iy * xDim + ix);
						distList += ix.toString() + ";" + iy.toString() + ";"+ iz.toString() +";" + b.toString()
					}
				}
			}  

	  /**
      * Input: ListBuffer[String]
      * Output: RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), (brightnessValue: Int, category: String)]
      * Parses every line from the input list obtained form the dist file and creates an RDD with the x,y,z triplet as the key
      * and the brightness and its category as the value
     */
		var distPixels = sc.parallelize(distList.toList)
				.map(dist => ((Integer.parseInt(dist.split(";")(0)),Integer.parseInt(dist.split(";")(1)),
				    Integer.parseInt(dist.split(";")(2))), 
						(Integer.parseInt(dist.split(";")(3)),brightnessToCategory(Integer.parseInt(dist.split(";")(3))))))

		/**
		 * Input: RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), (brightnessValue: Int, category: String)] and
		 * RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), brightnessValue: Int]
     * Output: RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), (brightnessFromImage: Int, (brightnessValue: Int, category: String))]
     * Joins the image and dist pair RDDs to get every pixel, its brightness and whether it belongs to foreground or 
     * background to a single pair RDD
		 */
		var resultPixels = imagePixels
				.join(distPixels).filter(pixel => (!pixel._2._2._2.equals("unknown")))

		/**
		 * Input: RDD[(xCoordinate: Int,yCoordinate: Int, zCoordinate: Int), (brightnessFromImage: Int, (brightnessValue: Int, category: String))]
		 * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
		 * Generates the neighbourhood vector for each pixel by calling findNeighbours method
		 */
		var RDDWithNeighbours = resultPixels
		                                    .map(pixel => (pixel._1, (pixel._2._1,pixel._2._2._2, 
		                                                    findNeighbours(imageDetails, pixel._1._1, pixel._1._2, pixel._1._3,
								                                                        xDim, yDim, imageDetails.size/(xDim*yDim)))))




	  /**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 0 degree with mirroring (x,y,z) to (-x,y,z)
	   */
		var RDDRotateZeroWithMirror =  RDDWithNeighbours.map(pixel => 
		                                                 ((xDim-pixel._1._1, pixel._1._2, pixel._1._3), 
		                                                     (pixel._2._1, pixel._2._2, 
		                                                         findNeighbours(imageDetails, xDim-pixel._1._1, pixel._1._2,
		                                                             pixel._1._3, xDim, yDim, imageDetails.size/(xDim*yDim)))))

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 0 degree without mirroring (x,y,z) to (x,y,z)
	   */
		var RDDRotateZeroWithoutMirror = RDDWithNeighbours

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 90 degree with mirroring along y axis (x,y,z) to (-y,-x,z)
		*/
		var RDDRotate90WithMirror = RDDWithNeighbours.map(pixel => 
		                                                    ((yDim-pixel._1._2, xDim-pixel._1._1, pixel._1._3),
		                                                        (pixel._2._1, pixel._2._2, 
		                                                            findNeighbours(imageDetails, yDim-pixel._1._2, xDim-pixel._1._1,
		                                                                pixel._1._3, xDim, yDim, imageDetails.size/(xDim*yDim)))))

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 90 degree without mirroring along y axis (x,y,z) to (y,-x,z)
	   */
		var RDDRotate90WithoutMirror = RDDWithNeighbours.map(pixel => 
		                                                            ((pixel._1._2,xDim -pixel._1._1, pixel._1._3), (pixel._2._1, pixel._2._2,
		                                                                findNeighbours(imageDetails, pixel._1._2, xDim-pixel._1._1,
		                                                                    pixel._1._3, xDim, yDim, imageDetails.size/(xDim*yDim)))))

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 180 degrees with mirroring along y axis (x,y,z) to (x,-y,z)
	   */
		var RDDRotate180WithMirror = RDDWithNeighbours.map(pixel => ((pixel._1._1, yDim-pixel._1._2, pixel._1._3), 
		                                                               (pixel._2._1, pixel._2._2, findNeighbours(imageDetails,
		                                                                   pixel._1._1, yDim-pixel._1._2, pixel._1._3,
		                                                                   xDim, yDim, imageDetails.size/(xDim*yDim)))))

   	/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]		                                                                   
	   * Rotate by 180 degrees without mirroring along y axis (x,y,z) to (-x,-y,z)
	   */
		var RDDRotate180WithoutMirror = RDDWithNeighbours.map(pixel => ((xDim-pixel._1._1,yDim -pixel._1._2, pixel._1._3), 
		                                                                  (pixel._2._1, pixel._2._2,
		                                                                      findNeighbours(imageDetails,
		                                                                          xDim-pixel._1._1, yDim-pixel._1._2,
		                                                                          pixel._1._3,
		                                                                          xDim, yDim, imageDetails.size/(xDim*yDim)))))

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 270 degrees with mirroring along y axis (x,y,z) to (y,x,z)
	   */
		var RDDRotate270WithMirror = RDDWithNeighbours.map(pixel => ((pixel._1._2, pixel._1._1, pixel._1._3), 
		                                                                (pixel._2._1, pixel._2._2,
		                                                                    findNeighbours(imageDetails, pixel._1._2, pixel._1._1,
		                                                                        pixel._1._3, xDim, yDim, imageDetails.size/(xDim*yDim)))))

		/**
	   * Input: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Output: RDD[(Int,Int,Int_,(Int,String,List[Int])]
	   * Rotate by 270 degrees without mirroring along y axis (x,y,z) to (-y,x,z)
	   */
		var RDDRotate270WithoutMirror = RDDWithNeighbours.map(pixel => ((-pixel._1._2, pixel._1._1, pixel._1._3), 
		                                                                  (pixel._2._1, pixel._2._2, findNeighbours(imageDetails,
		                                                                      yDim-pixel._1._2, pixel._1._1, pixel._1._3,
		                                                                      xDim, yDim, imageDetails.size/(xDim*yDim)))))  

		RDDWithNeighbours.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_neighours")
		RDDRotateZeroWithMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate0_mirror")
		RDDRotateZeroWithoutMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate0_nomirror")
		RDDRotate90WithMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate90_mirror")
		RDDRotate90WithoutMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate90_nomirror")
		RDDRotate180WithMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate180_mirror")
		RDDRotate180WithoutMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate180_nomirror")
		RDDRotate270WithMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate270_mirror")
		RDDRotate270WithoutMirror.coalesce(1).saveAsTextFile(args(4)+"/"+imageNumber+"_rotate270_nomirror")
		val duration = (System.nanoTime - t1) / 1e9d
		println(duration)
		RDDWithNeighbours = sc.emptyRDD
		RDDRotateZeroWithMirror = sc.emptyRDD
		RDDRotateZeroWithoutMirror = sc.emptyRDD
		RDDRotate90WithMirror = sc.emptyRDD
		RDDRotate90WithoutMirror = sc.emptyRDD
		RDDRotate180WithMirror = sc.emptyRDD
		RDDRotate180WithoutMirror = sc.emptyRDD
		RDDRotate270WithMirror = sc.emptyRDD
		RDDRotate270WithoutMirror = sc.emptyRDD
		
		imagePixels = sc.emptyRDD 
		distPixels = sc.emptyRDD
		resultPixels = sc.emptyRDD
		imageDetails = Array()
		distDetails = Array()
	}
}
  
  /**
   * brightnessToCategory: method to classify the distance value of the pixel
   * @param: brightness
   * If the distance value is 0 or 1, set it to “foreground”. 
   * If the distance value is greater than 3, set it to “background”.
   * For all other distance values, i.e., 2 and 3, the class is unknown. Such records should not be used for training or testing. Hence you can discard them.
   */
  def brightnessToCategory(brightness: Int) = {
    brightness match {
		  case t if t == 0 || t == 1 => "foreground"
		  case t if t > 3 => "background"
		  case _ => "unknown"
		  }
  }
  
  /**
   * findNeighbours: method to find the neighbourhood pixels for a given input pixel based on the 
   * size of the nieghbourhood, the x,y,z coordinates and the width and hieght of the image
   * @param: imageDetails: bytes array representation of the image
   * @param: xDimension, yDimension and zDimension of the image
   * @param: x,y,z coordinates of the pixel
   */
  def findNeighbours(imageDetails: Array[String], xDimension: Int, yDimension: Int, zDimension: Int, xDim: Int,
	  yDim: Int, zDim: Int): List[String] = {
      import util.control.Breaks._
			var vector = new ListBuffer[String]()
			for(xx <- -maxX/2 to maxX/2){
				for(yy <- -maxY/2 to maxY/2){
					for(zz <- -maxZ/2 to maxZ/2){
						breakable {
							if(xx == 0 && yy == 0 && zz == 0){
								break
							}

							if (isOnMap(xDimension + xx, yDimension + yy, zDimension + zz, xDim, yDim)) {
							  if((((zDimension+zz) * yDim * xDim) + ((yDimension+yy) * xDim) + (xDimension+xx))  < (xDim * yDim * zDim)){
								  vector += imageDetails(((zDimension+zz) * yDim * xDim) + ((yDimension+yy) * xDim) + (xDimension+xx))
								}
							}
						}
					}
				}
			}
			
      if (isOnMap(xDimension, yDimension, zDimension, xDim, yDim)) {
			  if(((zDimension * yDim * xDim) + (yDimension * xDim) + xDimension)  < (xDim * yDim * zDim)){
				  vector += imageDetails((zDimension * yDim * xDim) + (yDimension * xDim) + xDimension)
				}
			}

			return vector.toList
  }
  
  
  /**
   * isOnMap: method to check if a given point is in the image
   * @param: x: x-Coordinate of the pixel
   * @param: y: y-Coordinate of the pixel
   * @param: z: z-Coordinate of the pixel
   * @param: xDim: x-Dimension of the image
   * @param: yDim: y-Dimension of the image
   */
  def isOnMap(x: Int, y: Int, z : Int, xDim: Int, yDim: Int): Boolean = {
    if(x >= 0 && y >= 0 && z >= 0 && x <= xDim && y <= yDim)
		  return true
		return false
  }
 
}
