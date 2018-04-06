package com.soso.mergetool

import netease.bigdata
import java.io.File

/*
 * hdfs小文件合并工具
 *
 * 支持parquet，gzip，lzo等文件格式且可扩展
 */
object MergeTool {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new IllegalArgumentException("args.length != 1")
    }
    new FileMerger().run(args)
  }
}
