import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.util

import com.xdc.hadoop.TileOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable, LongWritable}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.{immutable, mutable}



/**
  * Created by xiadc on 2018-03-08.
  */
object PartitionLosAlgorithm {

  //获取文件元信息
  def readMetaMsg(filePath: String) = {
    //读取本地文件
   /* val file = new File(filePath)
    val in = new FileInputStream(file)*/
    //读取hdfs文件
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(filePath),conf)
    val in = fs.open(new Path(filePath))

    val bytes = new Array[Byte](4)
    val bytes8 = new Array[Byte](8)

    val calDemInfo = new CalDemInfo()

    //1.如何得到DEM文件的左下角经纬度坐标xmin,ymin
    //2.如何得到DEM文件的网格间距（每两个像素之间的经纬度数）xgap和ygap
    //3.如何得到DEM文件的元素个数的行列数nx，ny
    //4.根据步骤1、2、3设置CalDemInfo的值
    in.read(bytes) //读出文件头4个字节
    //读出nx
    in.read(bytes)
    calDemInfo.nx = ByteBuffer.wrap(bytes.reverse).getInt()
    //读出ny
    in.read(bytes)
    calDemInfo.ny = ByteBuffer.wrap(bytes.reverse).getInt()
    //读出xmin
    in.read(bytes8)
    calDemInfo.xmin = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出xmax
    in.read(bytes8)
    calDemInfo.xmax = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出ymin
    in.read(bytes8)
    calDemInfo.ymin = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出ymax
    in.read(bytes8)
    calDemInfo.ymax = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //关闭输入流
    in.close()
    //计算xgap，ygap
    calDemInfo.xgap = (calDemInfo.xmax - calDemInfo.xmin) / (calDemInfo.nx - 1)
    calDemInfo.ygap = (calDemInfo.ymax - calDemInfo.ymin) / (calDemInfo.ny - 1)

    calDemInfo.leftDown = new RasPos(0,0)
    calDemInfo.rightDown = new RasPos(calDemInfo.nx-1,0)
    calDemInfo.rightUp = new RasPos(calDemInfo.nx -1,calDemInfo.ny -1)
    calDemInfo.leftUp = new RasPos(0,calDemInfo.ny-1)

    println(calDemInfo.nx+" "+calDemInfo.ny+" "+ calDemInfo.xmin+" "+calDemInfo.xmax+" "+calDemInfo.ymin+" "+calDemInfo.ymax)
    calDemInfo
  }


  //main函数，程序的入口函数
  //参数表：0：输入文件的绝对路径，1：结果输出目录，2：可视域算法数据划分块数,
  def main(args: Array[String]): Unit = {
    if(args.length !=3){
      println("参数错误！")
      System.exit(-1)
    }

    //grid无效值
    val InvalidValue = 1.70141E38f
    //判断两个double类型的值的大小用
    val eps = 1E-6
    //设置视点坐标
    val eyePnt = new Point()
    //demo47视点
    /*eyePnt.x = 393820.0d
    eyePnt.y = 3258990.0d
    eyePnt.z = 600.0d*/
    //hubei视点
    eyePnt.x = 114.0d
    eyePnt.y = 30.5d
    eyePnt.z = 500.0d
    //shanxi视点
    /*eyePnt.x = 108.84d
    eyePnt.y = 34.53d
    eyePnt.z = 2800.0d*/
    //qinghai视点
    /*eyePnt.x = 98.804d
    eyePnt.y = 36.227d
    eyePnt.z = 6300.0d*/
    //xizang视点
    /*eyePnt.x = 88.655d
    eyePnt.y = 30.808d
    eyePnt.z = 8000.0d*/
    //xinjiang视点
    /*eyePnt.x = 84.962d
    eyePnt.y = 42.233d
    eyePnt.z = 3500.0d*/

    //sparkContext环境设置
    val conf = new SparkConf().setAppName("LOS_Spark_Algorithm").setMaster("spark://master:7077")
   /* val conf = new SparkConf().setAppName("LOS_Spark_Algorithm").setMaster("local")*/
    //使用kryo序列化方式
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //严格要求注册类，不注册的类序列化时会报错
    conf.set("spark.kryo.registrationRequired", "true")
    //注册需要序列化的类对象
    conf.registerKryoClasses(Array(classOf[CalDemInfo],classOf[PartitionMetaMsg],classOf[RasPos],classOf[PointD],classOf[Point]))
    //设置spark最大分区字节数为2G
    conf.set("spark.files.maxPartitionBytes","2048m")
    conf.set("spark.kryoserializer.buffer.max","512m")
    //压缩序列化的rdd分区，消耗一些cpu，减少空间的使用
    conf.set("spark.rdd.compress","true")
   /* conf.set("spark.shuffle.memoryFraction","0.4")
    conf.set("spark.storage.memoryFraction","0.4")*/

    val sc = new SparkContext(conf)
    //val filePath = "D:\\sparkTestData\\HubeiDEM.Ras"
    //读入文件输入路径
    val filePath = args(0)
    //读取数据元信息,存入calDemInfo中
    val calDemInfo = readMetaMsg(filePath)

    //计算节点个数
    val calNodeNum = args(2).toInt
    //数据划分的分区信息存放在partitionMsgArray中
    val partitionMsgArray = new Array[PartitionMetaMsg](calNodeNum)

    println("-----输入路径:"+args(0)+" --------------")
    println("-----输出路径:"+args(1)+" --------------")
    println("-----分区个数:"+partitionMsgArray.length+" --------------")
    println("-----视点信息:x="+eyePnt.x+" y="+eyePnt.y+ " z=" + eyePnt.z +"--------------")
    println("元数据信息："+calDemInfo.nx+" "+calDemInfo.ny+" "+ calDemInfo.xmin+" "+calDemInfo.xmax+" "+calDemInfo.ymin+" "+calDemInfo.ymax)

    val xmin: Array[Double] = new Array[Double](2)
    val ymin: Array[Double] = new Array[Double](2)
    val xtap: Array[Double] = new Array[Double](2)
    val ytap: Array[Double] = new Array[Double](2)
    val nx: Array[Int] = new Array[Int](2)
    val ny: Array[Int] = new Array[Int](2)

    //isvisible函数中交换使用
    xmin(0) = calDemInfo.xmin
    ymin(1) = calDemInfo.xmin

    ymin(0) = calDemInfo.ymin
    xmin(1) = calDemInfo.ymin

    xtap(0) = calDemInfo.xgap
    ytap(1) = calDemInfo.xgap

    ytap(0) = calDemInfo.ygap
    xtap(1) = calDemInfo.ygap

    nx(0) = calDemInfo.nx
    ny(1) = calDemInfo.nx

    nx(1) = calDemInfo.ny
    ny(0) = calDemInfo.ny


    /*-----------------------下面是需要使用的函数定义--------*/
    //坐标点(x1,y1)与(x2,y2)之间的距离()
    val distance = (x1: Double, y1: Double, x2: Double, y2: Double) => Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2))

    //判断a是否等于b
    val doubleEquals = (a: Double, b: Double) => Math.abs(a - b) <= eps

    //判断a是否小于或等于b
    val doubleLEquals = (a: Double, b: Double) => (Math.abs(a - b) <= eps) || (a - b <= 0)

    //float转字节数组
    val getFloatByte = (value: Float) => {
      val byteBuffer = ByteBuffer.allocate(4)
      byteBuffer.asFloatBuffer().put(value)
      byteBuffer.array.reverse.array
    }

    //高程z值转换，（z-factor），x，y是经纬度，z是米，需要将z值转换为经纬度高程值
    def transferZValue(py: Double, pz: Double)={
      var realPz = pz
      if(py > 90.0d){
        //啥都不做
      }else {
        if (0.0d <= py && py <= 5.0d) {
          //赤道
          realPz = pz * 0.00000898d
        } else if (5.0d < py && py <= 15.0d) {
          realPz = pz * 0.00000912d
        } else if (15.0d < py && py <= 25.0d) {
          realPz = pz * 0.00000956d
        } else if (25.0d < py && py <= 35.0d) {
          realPz = pz * 0.00001036d
        } else if (35.0d < py && py <= 45.0d) {
          realPz = pz * 0.00001171d
        } else if (45.0d < py && py <= 55.0d) {
          realPz = pz * 0.00001395d
        } else if (55.0d < py && py <= 65.0d) {
          realPz = pz * 0.00001792d
        } else if (65.0d < py && py <= 75.0d) {
          realPz = pz * 0.00002619d
        } else if (75.0d < py && py <= 90.0d) {
          realPz = pz * 0.00005156d
        }
      }
      realPz
    }

    //LOS可视性算法实现，输入参数是目标点的坐标与高程值
    def isVisible(px: Double, py: Double, pz: Double,hashmap:mutable.HashMap[Long,Float]): Boolean = {

      var dx, dy, k, x0, y0, x1, x, y, dz, uz, z, phi, ydelt = 0.0
      var i, si, ei, j, No = 0L
      var swapFlag = 0
      var FoundFlag = false
      var temp = new Point()
      val pnt = new Array[Point](2)
      //深拷贝视点数据，防止视点变化
      pnt(0) = new Point
      pnt(0).x = eyePnt.x
      pnt(0).y = eyePnt.y
      pnt(0).z = eyePnt.z

     // pnt(0) = eyePnt
      pnt(1) = new Point

      pnt(1).x = px
      pnt(1).y = py
      pnt(1).z = pz

      //保证视点比目标点低
      if (pnt(0).z > pnt(1).z) {
        temp = pnt(0);
        pnt(0) = pnt(1);
        pnt(1) = temp
      }
      //统一两点连线斜率到一种情况
      dx = pnt(1).x - pnt(0).x
      dy = pnt(1).y - pnt(0).y
      if (Math.abs(dy) > Math.abs(dx)) {
        //X和Y互换
        x0 = dx;
        dx = dy;
        dy = x0
        x0 = pnt(0).x;
        pnt(0).x = pnt(0).y;
        pnt(0).y = x0
        x0 = pnt(1).x;
        pnt(1).x = pnt(1).y;
        pnt(1).y = x0
        swapFlag = 1
      }

      //起始点坐标
      x0 = Math.min(pnt(0).x, pnt(1).x)
      x1 = Math.max(pnt(0).x, pnt(1).x)
      y0 = if (doubleEquals(x0, pnt(0).x)) pnt(0).y else pnt(1).y
      k = dy / dx

      //加入指定范围的考虑
      si = ((x0 - xmin(swapFlag)) / xtap(swapFlag)).toLong
      if (si < 0) si = 0
      if (si > (if (swapFlag == 0) calDemInfo.nx - 1 else calDemInfo.ny - 1)) si = if (swapFlag == 0) calDemInfo.nx - 1 else calDemInfo.ny - 1

      ei = ((x1 - xmin(swapFlag)) / xtap(swapFlag)).toLong + 1
      if (ei < 0) ei = 0
      if (ei > (if (swapFlag == 0) calDemInfo.nx - 1 else calDemInfo.ny - 1)) ei = if (swapFlag == 0) calDemInfo.nx - 1 else calDemInfo.ny - 1

      //视线倾角.
      phi = (pnt(1).z - pnt(0).z) / distance(pnt(0).x, pnt(0).y, pnt(1).x, pnt(1).y)
      x = xmin(swapFlag) + si * xtap(swapFlag)

      //起始Y及步长.
      y = y0 + k * (x - x0)
      ydelt = k * xtap(swapFlag)

      for (i <- si + 1 to ei - 1 if !FoundFlag) {
        x += xtap(swapFlag)
        y += ydelt

        j = ((y - ymin(swapFlag)) / ytap(swapFlag)).toLong

        //越界处理
        if (swapFlag == 0) {
          j = if ((j > (calDemInfo.ny - 2))) (calDemInfo.ny - 2) else j
        } else {
          j = if ((j > (calDemInfo.nx - 2))) (calDemInfo.nx - 2) else j
        }
        if (swapFlag == 0) {
          No = j * calDemInfo.nx + i + 1
          //分区的边界值处理，如果需要的计算点不在该分区中，则将需要的点设置为无效值
          if(hashmap.contains(No)){
            dz = hashmap.get(No).get
          }else{
            dz = InvalidValue
          }
          if(hashmap.contains(No + calDemInfo.nx)){
            uz = hashmap.get(No + calDemInfo.nx).get
          }else{
            uz = InvalidValue
          }

        } else {
          No = i * calDemInfo.nx + j + 1
          //分区的边界值处理，如果需要的计算点不在该分区中，则将需要的点设置为无效值,即默认该点可视
          if(hashmap.contains(No)){
            dz = hashmap.get(No).get
          }else{
            dz = InvalidValue
          }
          if(hashmap.contains(No + 1)){
            uz = hashmap.get(No + 1).get
          }else{
            uz = InvalidValue
          }

        }
        /*dz = if (swapFlag == 0) CalGrdZValue(No = j * CalDemInfo.nx + i + 1) else CalGrdZValue(No = i * CalDemInfo.nx + j + 1) //CalGrdZValue是DEM高程值数组.
            uz = if (swapFlag == 0) CalGrdZValue(No + CalDemInfo.nx) else CalGrdZValue(No + 1)*/

        //无效点作为可视情况处理,跳过.(暂不考虑Surfer无效值)
        if (doubleEquals(dz, InvalidValue) && doubleEquals(uz, InvalidValue)) {
          //不处理，当做可视
        } else {
          if(dz != InvalidValue && uz != InvalidValue) {
            //z值转换，对获取的高程值根据纬度转换，然后参与后续计算
            // dz = transferZValue(y,dz)
            // uz = transferZValue(y,uz)
            //线性插值.(Down,Upper)
            z = dz + (y - (ymin(swapFlag) + j * ytap(swapFlag))) * (uz - dz) / ytap(swapFlag)
          }else if(dz != InvalidValue){//dz是有效值，uz无效值
            z = dz
          }else {////dz是有效值，uz无效值
            z = uz
          }

          if (doubleLEquals(z, phi * distance(pnt(0).x, pnt(0).y, x, y) + pnt(0).z)) {
            //计算结果可视
          } else {
            //计算结果不可视
            FoundFlag = true
          }
        }
      }
      FoundFlag
    }


    def doLOS(keyValue: (Int, mutable.HashMap[Long,Float])) = {
      //5.将当前像素当做目标点，计算是否可视，
      // 5.1 获取每个像素的经纬度坐标和高程值
      //获取高程值
      /*val elevation = ByteBuffer.wrap(keyValue._2.reverse).getFloat()*/

      val key = keyValue._1 //key值，也就是分区号
      /* val realPos = keyValue._2._1//当前像素真实偏移值
       val evevation = ByteBuffer.wrap(keyValue._2._2.reverse).getFloat() //当前元素的具体值，即高程值*/

      val hashmap = keyValue._2
      var px = 0.0
      var py = 0.0


      val resultByteHashMap = new mutable.HashMap[Long,Byte]()
      //遍历该可变集合hashmap，对每个键值对处理
      for ((currentPos,elevation) <- hashmap) {
       // var tempByteArray = new Array[Byte](4)
        var tempByte:Byte = 2
        if(doubleEquals(elevation,InvalidValue)) {//高程值无效值,默认结果用2
         // tempByteArray = getFloatByte(InvalidValue)
        }else {
          px = calDemInfo.xmin + (currentPos % calDemInfo.nx) * calDemInfo.xgap
          py = calDemInfo.ymin + (currentPos / calDemInfo.nx) * calDemInfo.ygap
          //z值转换
         // val realElevation = transferZValue(py,elevation)
          val realElevation = elevation
          val flag = isVisible(px, py, realElevation,hashmap)//true不可视，false可视
          tempByte = if (flag) 0 else 1
        }
        resultByteHashMap.put(currentPos,tempByte)
      }
      //返回键为1，
      (1L,resultByteHashMap)
    }
    //字节数组转float数组
    def transform(keyValue: (String, Array[Byte])) = {
      //将Byte数组转为Float数组
      val floatArrayLength = (keyValue._2.length - 60) / 4
      val calGrdZValue = new Array[Float](floatArrayLength)
      val tmpBytes = new Array[Byte](4)
      for (i <- 0 to floatArrayLength -1) {
        tmpBytes(0) = keyValue._2(60 + i * 4)
        tmpBytes(1) = keyValue._2(61 + i * 4)
        tmpBytes(2) = keyValue._2(62 + i * 4)
        tmpBytes(3) = keyValue._2(63 + i * 4)
        calGrdZValue(i) = ByteBuffer.wrap(tmpBytes.reverse).getFloat()
      }

      (keyValue._1, calGrdZValue)
    }

    //根据分区个数计算每个分区应该拥有的像素个数
    //row 和col: dem数据的行列数
    def getEveryGridNum (nx:Int,ny:Int, partitionNum:Int):Double = {
      (nx * ny).toDouble/partitionNum.toDouble
    }

    //将dem像素的一维数组下标转换为二维坐标，便于计算
    def transferToTwoDimensionalCoordinates(index :Long,nx:Int,ny:Int) = {
      new RasPos((index % nx).toInt ,(index / nx).toInt)
    }
    //将视点数据十进制经纬度坐标转换为栅格数据二维坐标，近似取离视点最近的左下角像素坐标
    def transferToTwoDimensionalCoordinatesFromPoint(pnt:Point) = {
      val xgap = pnt.x - calDemInfo.xmin
      val ygap = pnt.y - calDemInfo.ymin
      val xdelt = (xgap/calDemInfo.xgap).toInt
      val ydelt = (ygap/calDemInfo.ygap).toInt
      new RasPos(xdelt,ydelt)
    }

    //计算分区划分的起始坐标
    //everyGridNum是每个分区拥有的像素数，num表示分区标号，左下角开始划分
    def getPartitionStartCoordinate(nx:Int,ny:Int,everyGridNum:Double,rank:Int) ={
      //定义起始坐标
      val startRasPos = new RasPos(0,0)

      if(rank != 0) { //第一个分块的起点设置为左下角点（0,0）

        //计算视点的栅格数据二维坐标
        val rasEyePnt = transferToTwoDimensionalCoordinatesFromPoint(eyePnt)
        //各分区分配栅格数的累计量
        val rankGridNum = rank * everyGridNum
        //视点与矩形顶点组成的三角形栅格数
        val oneGridNum = (nx * rasEyePnt.rasY).toDouble / 2
        val twoGridNum = (ny * (nx - rasEyePnt.rasX )) / 2 + oneGridNum
        val threeGridNum = (nx * (ny - rasEyePnt.rasY)) / 2 + twoGridNum

        //确定分区的起始点
        if (oneGridNum >= rankGridNum) {
          //起始点在第一个三角形中
          startRasPos.rasY = 0
          startRasPos.rasX = (2*rankGridNum/rasEyePnt.rasY).toInt - 1
        }else if(twoGridNum >= rankGridNum){
          //起始点在第二个三角形中
          startRasPos.rasY = ((rankGridNum - oneGridNum)*2/(nx - rasEyePnt.rasX)).toInt - 1
          startRasPos.rasX = nx - 1
        }else if(threeGridNum >= rankGridNum){
          //起始点在第三个三角形中
          startRasPos.rasY = ny - 1
          startRasPos.rasX = nx - ((rankGridNum - twoGridNum)*2/(ny - rasEyePnt.rasY)).toInt - 1
        }else{
          //起始点在第四个三角形中
          startRasPos.rasY = ny - ((rankGridNum-threeGridNum)*2/rasEyePnt.rasX).toInt -1
          startRasPos.rasX = 0
        }
      }
      startRasPos
    }

    //辅助函数，利用面积法判断一个点是否在三角形内部（包括边界上）
    def isInTriangle(a:RasPos,b:RasPos,c:RasPos,p:RasPos):Boolean = {
      val abc = triangleArea(a,b,c)
      val abp = triangleArea(a,b,p)
      val acp = triangleArea(a,c,p)
      val bcp = triangleArea(b,c,p)
      val flag =  if(doubleEquals(abc,abp+acp+bcp)) true else false
      flag
    }

    //由三个点计算这三点组成的三角形面积
    def triangleArea(a:RasPos,b:RasPos,c:RasPos):Double = {
      Math.abs((a.rasX*b.rasY + b.rasX*c.rasY + c.rasX*a.rasY - b.rasX*a.rasY - c.rasX*b.rasY - a.rasX*c.rasY)/2.0d)
    }

    //计算每个分区的起始坐标和终止坐标
    def getPartitionMetaMsg(calNodeNum:Int) ={
      //计算每个分区应该拥有的像素个数
      val everyGridNum = getEveryGridNum(calDemInfo.nx,calDemInfo.ny,calNodeNum)
      // val partitionMsgArray = new Array[PartitionMetaMsg](calNodeNum)
      for(i <- 0 until calNodeNum){
        //分区编号
        val no = i
        val startPos = getPartitionStartCoordinate(calDemInfo.nx,calDemInfo.ny,everyGridNum,i)
        val endPos = getPartitionStartCoordinate(calDemInfo.nx,calDemInfo.ny,everyGridNum,(i+1)%calNodeNum)
        partitionMsgArray(i) = new PartitionMetaMsg(no,startPos,endPos)
      }
    }

    //根据栅格点二维坐标判断该点属于哪个分区
    def getPartitionNum(currentRasPos:RasPos,partitionMsgArray:Array[PartitionMetaMsg]) ={
      var flag = false
      var partitionNum = 0
      //计算视点的栅格数据二维坐标
      val rasEyePnt = transferToTwoDimensionalCoordinatesFromPoint(eyePnt)
      for(i <- 0 to partitionMsgArray.length - 1 if !flag){
        partitionNum = i
        val startRasPos = partitionMsgArray(i).startPos
        val endRasPos = partitionMsgArray(i).endPos
        //判断当前分区是不是三角形，根据起始点和终止点是否在同一边界线上来判断
        if((startRasPos.rasX == endRasPos.rasX) ||(startRasPos.rasY == endRasPos.rasY)) {//起始点和终止点在同一边界线上，说明当前分区是一个三角形
          flag = isInTriangle(startRasPos, endRasPos, rasEyePnt, currentRasPos)
        }else if((startRasPos.rasY == 0) && (endRasPos.rasX == calDemInfo.nx - 1)){//右下角三角形:起点下，终点右
          flag = isInTriangle(startRasPos,calDemInfo.rightDown,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightDown,endRasPos,rasEyePnt,currentRasPos)
        }else if((startRasPos.rasX == calDemInfo.nx - 1)&&(endRasPos.rasY == calDemInfo.ny -1)){//右上角三角形：起点右，终点上
          flag = isInTriangle(startRasPos,calDemInfo.rightUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightUp,endRasPos,rasEyePnt,currentRasPos)
        }else if((startRasPos.rasY == calDemInfo.ny - 1)&&(endRasPos.rasX == 0)){//左上角三角形：起点上，终点左
          flag = isInTriangle(startRasPos,calDemInfo.leftUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.leftUp,endRasPos,rasEyePnt,currentRasPos)
        }else if((startRasPos.rasY == 0)&&(endRasPos.rasY == calDemInfo.ny - 1)){//起点下，终点上
          flag = isInTriangle(startRasPos,calDemInfo.rightDown,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightDown,calDemInfo.rightUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightUp,endRasPos,rasEyePnt,currentRasPos)
        }else if((startRasPos.rasX == calDemInfo.nx -1)&&(endRasPos.rasX == 0)){//起点右，终点左
          flag = isInTriangle(startRasPos,calDemInfo.rightUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightUp,calDemInfo.leftUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.leftUp,endRasPos,rasEyePnt,currentRasPos)
        }else{//起点下，终点左
          flag = isInTriangle(startRasPos,calDemInfo.rightDown,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightDown,calDemInfo.rightUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.rightUp,calDemInfo.leftUp,rasEyePnt,currentRasPos) || isInTriangle(calDemInfo.leftUp,endRasPos,rasEyePnt,currentRasPos)
        }
      }
      partitionNum
    }

    //将分到同一个分区的像素的key设置为分区编号，如1,2,3等
    def reDefineKey(keyValue:(Long,Array[Byte])) ={
      //得到当前键值对的栅格点二维坐标
      val currentRasPos = transferToTwoDimensionalCoordinates(keyValue._1,calDemInfo.nx,calDemInfo.ny)
      //判断该二维坐标属于哪个分区
      val key = getPartitionNum(currentRasPos,partitionMsgArray)
      //将字节数组转换成Float
      val value = ByteBuffer.wrap(keyValue._2.reverse).getFloat()
      /*println("-----------------------key:"+key+" keyValuePos:"+keyValue._1+"-----------")*/
      (key,(keyValue._1,value))
    }

    //aggregateByKey操作的参数函数，该操作性能优于reduceByKey
    //1.按key值合并在同一个partition中的值
    def seqOp(hashmap:mutable.HashMap[Long,Float], b:(Long,Float))={
      hashmap += b

    }
    //2.按key值合并不同partition中的值
    def combOp(hashmap1:mutable.HashMap[Long,Float],hashmap2:mutable.HashMap[Long,Float])={
      hashmap1 ++= hashmap2
    }

    // reduceByKey执行函数，合并treeMap
    def combineHashMap(hashmap1: mutable.HashMap[Long,Array[Byte]],hashmap2: mutable.HashMap[Long,Array[Byte]])={
      hashmap1 ++= hashmap2
    }
    //treemap转成byte数组，方便持久化存储
    def transferHashMapToByteArray(keyValue:(Long,mutable.HashMap[Long,Byte])) ={
      val hashmap = keyValue._2
      val resultByteArray = new Array[Byte](hashmap.size)
      //将hashmap中的数据转换为字节数组
      for(i <- 0 to hashmap.size-1){
        resultByteArray(i) = hashmap.get(i).get
      }
      //键值对序列化
      (new LongWritable(keyValue._1), new BytesWritable(resultByteArray))
    }


    /*---------------------以上是所有函数定义-----------------------------------*/

    /*---------------------以下是RDD处理过程-----------------------------------*/

    //视点z值转换
   // eyePnt.z = transferZValue(eyePnt.y,eyePnt.z)
    //获取分区信息
    getPartitionMetaMsg(calNodeNum)



    //生成rdd
    val gridRdd = sc.binaryRecords(args(0),4)
    gridRdd.zipWithIndex().map(keyValue=>(keyValue._2 - 15,keyValue._1)) //转换key为0开始的键值对，
                        .filter(keyValue=>keyValue._1>=0)  //过滤掉头信息
                         .repartition(8)//重分区，增加redefineKey的并行度
                          .map(reDefineKey)//重新按照所属分区定义key值
                            .aggregateByKey(new mutable.HashMap[Long,Float](),args(2).toInt)(seqOp,combOp)//默认分区方式为HashPartitioner，分区个数为calNodeNum,运用hashmap数据结构是为了提高后续计算的按键的查找效率
                              .map(doLOS)//执行基于视线的可视域算法
                                .reduceByKey((v1,v2)=>(v1 ++=v2),1)//合并所有分区的多个map的结果,合并后分区设置为1
                                  .map(transferHashMapToByteArray)//结果转换为字节数组
                                    .saveAsNewAPIHadoopFile(args(1), classOf[LongWritable], classOf[BytesWritable], classOf[TileOutputFormat]) //持久化结果数据*/
    println("compute success!")
    /*---------------------以上是RDD处理过程，main函数结束-----------------------------------*/
  }
}


/*---------------------以下是定义的辅助类-----------------------------------*/

//表示点的十进制经纬度坐标和高程值
class Point extends Serializable{
  var x: Double  = 0.0
  var y: Double = 0.0
  var z: Double = 0.0
}

class PointD(val pos: Long, val z: Float) extends Serializable{}

//表示栅格点坐标
class RasPos(var rasX:Int,var rasY:Int) extends Serializable{}

//分区元信息，记录每个分区的编号、起始坐标以及终止坐标
class PartitionMetaMsg(var No:Int, var startPos:RasPos, var endPos:RasPos) extends Serializable

//栅格数据元数据信息存储
class CalDemInfo extends Serializable{
  //dem左下角
  var xmin: Double = 0.0
  var ymin: Double = 0.0

  var xmax: Double = 0.0
  var ymax: Double = 0.0

  //可视域分析网格间距，默认与DEM一样
  var xgap: Double = 0.0
  var ygap: Double = 0.0
  //dem行列数
  var nx: Int = 0
  var ny: Int = 0

  //四个顶点栅格坐标
  var leftDown = new RasPos(0,0)
  var rightDown = new RasPos(0,0)
  var rightUp = new RasPos(0,0)
  var leftUp = new RasPos(0,0)
}

