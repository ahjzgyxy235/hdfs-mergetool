package com.netease.mergetool

import java.util
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Stream.consWrapper
import scala.collection.mutable.ArrayBuffer

class FileMerger {

  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  val workPath = "/tmp/hdfs_mergetool" // 集群tmp目录
  val TOTAL_LEN_THRESHOLD: Long = 1024L * 1024 * 1024 // 1gb
  val BLOCK_SIZE: Long = 256L * 1024 * 1024

  // 线程池并发提交job
  val pool = new ThreadPoolExecutor(1, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable]())

  val sparkConf = new SparkConf().setAppName("Hubble small hdfs files merge task")
                                 .set("spark.sql.parquet.compression.codec gzip", "gzip")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  
  // 保证parquet使用gzip进行压缩
  sqlContext.sql("set parquet.compression=gzip;")
  sc.hadoopConfiguration.set("parquet.compression","gzip")
  
  val fileSystem = FileSystem.get(sc.hadoopConfiguration)

  def run(args: Array[String]): Unit = {
    
    fileSystem.mkdirs(new Path(workPath))
    
    var allTaskCtxs:Stream[TaskContext] = null
    
    // 获取参数中配置的待合并目录
    val dbPaths = getDbPathFromArgs(args)
    
    // 遍历待合并目录找出所有需要合并的子目录，生成对应的task
    for(dbPath <- dbPaths){
        if(null==allTaskCtxs){
           allTaskCtxs = getPartitions(dbPath)
        }  else {
           allTaskCtxs = allTaskCtxs.append(getPartitions(dbPath))
        }
    }
    
    // 保障所有tmp目录下不存在残留文件，如果存在则先删除之
    val partitions = allTaskCtxs.filter(context => {
      val f = context.inputPath
      if (!context.tmpPath.toString.contains(workPath)) {
        // It's a programmatic error, shouldn't happen.
        LOG.error("tmp path {} is invalid! filtered..", f)
        false
      } else {
        // clean up and prepare
        if (fileSystem.exists(context.tmpPath)) {
          LOG.warn("tmp path {} exists! removing it and then we can restart the task", context.tmpPathStr)
          fileSystem.delete(context.tmpPath, true)
        }
        true
      }
    })
    
    // 提交任务
    LOG.info("submit "+partitions.size+" tasks.")
    for (context <- partitions) {
      LOG.info("Task info ============== ")
      LOG.info("inputPath: "+context.inputStr)
      LOG.info("inputSize: "+context.inputFiles.size)
      LOG.info("tmpPath: "+context.tmpPathStr)
      pool.submit(context.getTask())
    }
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        pool.shutdownNow()
      }
    }))
    
    pool.shutdown()
    LOG.info("waiting job to be finished!")
    pool.awaitTermination(1L, TimeUnit.HOURS)
    sc.stop()
    LOG.info("successfully finished!")
  }
  
  /**
   * 遍历每个父目录下的所有最小一层子目录
   */
  def getPartitions(curDir: Path): Stream[TaskContext] = {
    LOG.info("listing:" + curDir)
    val files = fileSystem.listStatus(curDir)
    var context: TaskContext = null

    val numDir = files.filter(f => f.isDirectory).length
    if (numDir == 0) {
      val taskType = getTaskType(files)
      if (taskType != UnknownTaskType) {
        // 待合并的文件  = 小于100M的文件
        val inputFiles = files.filter(taskType.inputFilesFilter)
        
        if(inputFiles.length==0){
            LOG.info("No small file was found in {}. Skipping..", curDir.toString)
        } else if(inputFiles.length==1){
            LOG.info("Only one small file was found in {}. Skipping..", curDir.toString)
        } else {
            LOG.info("Found partition " + curDir + " with inputFiles:" + inputFiles.length)
            context = createTaskContext(taskType, fileSystem, curDir, inputFiles, getTmpPath(curDir))
        }
      }
    }
    
    val subDirs = scala.util.Random.shuffle(files.filter(f => f.isDirectory).toSeq)
    val tailStream = subDirs.toStream.flatMap(f => getPartitions(f.getPath))
    context match {
      case null => tailStream
      case context: TaskContext => context #:: tailStream
    }
  }

  private def createTaskContext(taskType: TaskType, fileSystem: FileSystem,
                                curDir: Path,
                                inputFiles: Array[FileStatus],
                                tmpPath: Path) : TaskContext = {
    taskType match {
      case ParquetTaskType =>
        new ParquetTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case LzoTaskType =>
        new LzoTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case GzipTaskType =>
        new GzipTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case _ => null
    }
  }
  
  /**
   * 根据input path计算对应的tmp path
   */
  private def getTmpPath(input: Path): Path = {
    // 去除schema的path
    val relativePathStr = Path.getPathWithoutSchemeAndAuthority(input).toString
    val schema = input.toUri().getScheme
    val authority = input.toUri().getAuthority
   
    var tmpPath:Path = null
    if (relativePathStr.startsWith("/")) {
      tmpPath = new Path(workPath, "." + relativePathStr)
    } else {
      tmpPath = new Path(workPath, relativePathStr)
    }
    new Path(schema, authority, tmpPath.toString())
  }
  
  /**
   * 支持多目录配置, 以‘,’分割
   */
  private def getDbPathFromArgs(args: Array[String]): ArrayBuffer[Path] = {
    if (args == null || args.length != 1) {
      throw new IllegalArgumentException
    }
    
    val paths = ArrayBuffer[Path]()
    val dirs = args(0).split(",")
    for(dir <- dirs){
        LOG.info("Input dir: "+dir)
        paths += new Path(dir)
    }
    paths
  }
  
  private def getTaskType(files: Array[FileStatus]): TaskType = {
    val lzoCount = files.filter(f => f.getPath.getName.endsWith(".lzo")).length
    val gzipCount = files.filter(f => f.getPath.getName.endsWith(".gz")).length
    val parquetCount = files.filter(f => f.getPath.getName.endsWith(".parquet")).length
    val hasMultipleType = List(lzoCount, gzipCount, parquetCount).count(p => p > 0) > 1
    if (hasMultipleType) {
      UnknownTaskType
    } else if (lzoCount > 0) {
      LzoTaskType
    } else if (gzipCount > 0) {
      GzipTaskType
    } else if (parquetCount > 0) {
      ParquetTaskType
    } else if (files.length > 1 && files(0).getLen > 4) {
      // find one file with magic number "PAR1"
      // https://parquet.apache.org/documentation/latest/
      val bytes = new Array[Byte](4)
      val inputStream = fileSystem.open(files(0).getPath)
      inputStream.read(bytes)
      inputStream.close()
      if (util.Arrays.equals(ParquetTaskType.PARQUET_MAGIC_NUMBER, bytes)) {
        ParquetTaskType
      } else {
        UnknownTaskType
      }
    } else {
      UnknownTaskType
    }
  }
}
