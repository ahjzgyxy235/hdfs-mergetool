package com.soso.mergetool

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.slf4j.Logger

abstract class TaskContext(val fileMerger: FileMerger, val fileSystem: FileSystem,
                           val inputPath: Path,
                           val inputFiles: Array[FileStatus],
                           val tmpPath: Path,
                           val taskType: TaskType) {
  val LOG: Logger = fileMerger.LOG
  val inputStr: String = inputPath.toString
  val tmpPathStr: String = tmpPath.toString

  def readWriteFile() : Unit
  
  /**
   * file merge task
   */
  def getTask(): Runnable =  new Runnable {
      
    val alertPOPList = "" 
      
    // 合并任务分为3个步骤
    // step 1: 读取imput目录下的小文件，合并成大文件并输出到tmp目录。 如果在这一步任务失败，可以直接重启任务，不会产生数据异常
    // step 2: 将tmp目录下的合并后的大文件move到input目录。        如果在这一步任务失败，需要手动删除已经move的大文件然后重启任务，才能保证数据合并准确性
    // step 3: 删除input目录下的原始小文件。                     如果在这一步任务失败，需要手动删除input下的原始小文件，才能保证数据合并准确性，不需要重启任务
    override def run(): Unit = {
      
      if (!tmpPathStr.contains(fileMerger.workPath)) {
          LOG.warn("tmp path {} is invalid!", tmpPathStr)
          return
      }
      LOG.info("start writing, input:" + inputStr + ", tmp path:" + tmpPathStr)
      
      // step 1
      try {
          readWriteFile()    
      } catch {
          case e: Throwable => {
              // 如果这一步抛出异常，则只需要重启merge任务再处理该目录即可
              throw new RuntimeException("job failed in step 1 and input path: "+inputStr, e)
          } 
      }
      
      LOG.info("finished writing files")
    
      val tmpNewFiles = fileSystem.listStatus(tmpPath).filter(f => f.getPath.toString.endsWith(taskType.getTargetFileExtension))
          
      val targetPaths = taskType.newTargetPaths(inputPath, tmpNewFiles.length)
      
      if(tmpNewFiles.length == 0){
          LOG.error("No new file was found in tmpPath: "+tmpPath.toString())
          fileSystem.listStatus(tmpPath).foreach(x => LOG.warn("new file: " + x.getPath.toString))
          throw new RuntimeException("file lost in"+tmpPath.toString())
      }
      
      // step 2
      for(i <- 0.until(tmpNewFiles.length)) {
          try {
             fileSystem.rename(tmpNewFiles(i).getPath, targetPaths(i))
             LOG.info("renamed file " + tmpNewFiles(i).getPath + " to " + targetPaths(i))
          } catch {
              case e: Throwable => {
                  // 如果这一步抛出异常，需要手动删除上面target path下 rename的大文件，然后重启merge任务处理该目录
                  throw new RuntimeException("job failed in step 1 and input path: "+inputStr, e)
              }
          }
      }
    
      LOG.info("cleaning up...")
      LOG.info("deleting tmp path {}", tmpPath)
      fileSystem.delete(tmpPath, true)
      
      // step 3
      var deleteSuccess:Boolean = true
      inputFiles.foreach(f => {
          try {
              fileSystem.delete(f.getPath, false)
              LOG.info("deleting input file {}", f.getPath)
          } catch {
              case e: Throwable => {
                  deleteSuccess = false
                  LOG.error("fail to delete input file {}", f.getPath)  // 这里打印出来的文件都需要手动删除
              }
          }
      })
      
      if(!deleteSuccess){
          // 如果这一步抛出异常，需要手动删除上面error log中的原始小文件，不需要重启任务
          throw new RuntimeException("job failed in step 3 and input path: "+inputStr)
      }
      
      LOG.info("job succeeded!")  
    }
  }
}

class ParquetTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                         inputPath: Path,
                         inputFiles: Array[FileStatus],
                         tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, ParquetTaskType) {
  
  def newParquetRDD(src: String) = {
     fileMerger.sqlContext.read.parquet(src)
  }

  override def readWriteFile() = {
    var totalFileLength:Long = 0
    val inputFilePaths = inputFiles.map { f => {
       totalFileLength += f.getLen   
       f.getPath.toString()
    } }
    
    val totalFileDF = fileMerger.sqlContext.read.parquet(inputFilePaths : _*)
    
    // 每block size（压缩前）一个分区，输出一个文件
    val result = (totalFileLength / fileMerger.BLOCK_SIZE).toInt
    
    val partitionSize = if (totalFileLength <= fileMerger.BLOCK_SIZE * 1.1) 1 else (result+1)
    
    LOG.info("All input files byte size is {}, and write into {} target files!", totalFileLength, partitionSize)
    
    // parquet文件使用gzip进行压缩
    totalFileDF.coalesce(partitionSize).write
                                       .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                                       .option("compression", "gzip")
                                       .parquet(tmpPathStr)
  }
}

class LzoTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                     inputPath: Path,
                     inputFiles: Array[FileStatus],
                     tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, LzoTaskType) {

  override def readWriteFile() = {
    var totalFileLength:Long = 0
    val inputFilePaths = inputFiles.map { f => {
       totalFileLength += f.getLen   
       f.getPath.toString()
    } }
    
    val totalFileDF = fileMerger.sqlContext.read.textFile(inputFilePaths : _*)
    
    // 每每block size（压缩前）一个分区，输出一个文件
    val result = (totalFileLength / fileMerger.BLOCK_SIZE).toInt
    
    val partitionSize = if (totalFileLength <= fileMerger.BLOCK_SIZE * 1.1) 1 else (result+1)
    
    LOG.info("All input files byte size is {}, and write into {} target files!", totalFileLength, partitionSize)
    
    // spark sql 2.x 版本不再支持lzo压缩，所以这里lzo文件合并之后切换成gzip压缩
    // Codec [lzo] is not available. Known codecs are bzip2, deflate, uncompressed, lz4, gzip, snappy, none.
    
    totalFileDF.coalesce(partitionSize).write
                                       .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                                       .option("compression", "gzip")
                                       .text(tmpPathStr)  
  }
}


class GzipTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                      inputPath: Path,
                      inputFiles: Array[FileStatus],
                      tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, GzipTaskType) {

  override def readWriteFile() = {
    var totalFileLength:Long = 0
    val inputFilePaths = inputFiles.map { f => {
       totalFileLength += f.getLen   
       f.getPath.toString()
    } }
    
    val totalFileDF = fileMerger.sqlContext.read.textFile(inputFilePaths : _*)
    
    // 每每block size（压缩前）一个分区，输出一个文件
    val result = (totalFileLength / fileMerger.BLOCK_SIZE).toInt
    
    val partitionSize = if (totalFileLength <= fileMerger.BLOCK_SIZE * 1.1) 1 else (result+1)
    
    LOG.info("All input files byte size is {}, and write into {} target files!", totalFileLength, partitionSize)
    
    totalFileDF.coalesce(partitionSize).write
                                       .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                                       .option("compression", "gzip")
                                       .text(tmpPathStr)
    
  }
}
