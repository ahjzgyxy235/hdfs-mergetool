package com.soso.mergetool

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

abstract class TaskType {
  val SMALL_FILE_LEN_THRESHOLD: Long = 100L * 1024 * 1024 // 100MB
  def getTargetFileExtension: String
  def targetFilesFilter(f: FileStatus) : Boolean
  def inputFilesFilter(f: FileStatus): Boolean = {
    smallfileFilter(f)
  }
  def smallfileFilter(f: FileStatus): Boolean = {
    f.getLen < SMALL_FILE_LEN_THRESHOLD
  }
  def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path]
}

case object LzoTaskType extends TaskType{
   
  // spark sql 2.x 版本不再支持lzo压缩，所以这里lzo文件合并之后切换成gzip压缩
  // Codec [lzo] is not available. Known codecs are bzip2, deflate, uncompressed, lz4, gzip, snappy, none.
  override def getTargetFileExtension: String = ".gz"

  override def targetFilesFilter(f : FileStatus): Boolean = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(getTargetFileExtension)
  }
  
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + "__" +System.currentTimeMillis() + getTargetFileExtension)).toArray
  }
}

case object GzipTaskType extends TaskType{

  override def getTargetFileExtension: String = ".gz"

  override def targetFilesFilter(f : FileStatus): Boolean = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(getTargetFileExtension)
  }
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + "__" +System.currentTimeMillis() + getTargetFileExtension)).toArray
  }
}

case object ParquetTaskType extends TaskType{
  val PARQUET_MAGIC_NUMBER: Array[Byte] = "PAR1".getBytes(StandardCharsets.US_ASCII)
  
  override def getTargetFileExtension: String = ".parquet"

  override def targetFilesFilter(f : FileStatus): Boolean = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(getTargetFileExtension)
  }
  
  /**
   * 生成输出文件名
   * 这里为生成的文件名增加时间戳
   */
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + "__" +System.currentTimeMillis() + getTargetFileExtension)).toArray
  }
}

case object UnknownTaskType extends TaskType {
  override def getTargetFileExtension: String = null
  override def targetFilesFilter(f : FileStatus) = false
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = null
}
