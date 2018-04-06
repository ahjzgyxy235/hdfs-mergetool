## hdfs-mergetool

### 工具说明

* HDFS小文件合并工具，程序会自动根据源文件后缀识别文件压缩类型，合并后文件保留原始压缩类型

* 如果想要支持新的文件类型，可以很方便的进行扩展，目前已经支持parquet，gzip，lzo三种文件格式

* 任务传唯一参数，即待合并文件目录，如果有多个目录需要合并可以使用英文逗号进行分割

* 对于每一个输入目录，都会遍历到最深层次的子目录，所有的合并任务都是基于子目录进行的,合并后的文件会替换掉原始子目录下的小文件

* 工具只会合并100M以下的文件，大文件保持不变。小文件合并后会按照256MB进行切分成1个或者多个文件


### 使用方法

1. 下载代码并编译打包
2. 通过以下方式提交任务

```
 # 参数自行调整
  ./spark-submit --name hdfs file merge task \
                 --master yarn-cluster \
                 --driver-cores 2 \
                 --driver-memory 4g \
                 --num-executors 10 \
                 --executor-core 4 \
                 --executor-memory 10g \
                 --jars your lib path \
                 --class com.soso.mergetool.MergeTool \
                 your main jar $SOURCE_PATH

```