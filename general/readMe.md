#运行命令汇总
##第三章

- 通过URLStreamHandler实例以标准输出方式显示Hadoop文件系统的文件
    
    `hadoop jar general-1.0-SNAPSHOT.jar org.guoqing.map.URLCat hdfs://master:9000/user/dengguoqing/quangle.txt`

- 直接使用FileSystem以标准输出格式显示Hadoop文件系统中的文件
        
    `hadoop jar general-1.0-SNAPSHOT.jar org.guoqing.map.FileSystemCat hdfs://master:9000/user/dengguoqing/quangle.txt`

- 将本地文件复制到Hadoop文件系统

    `hadoop jar general-1.0-SNAPSHOT.jar org.guoqing.map.FileCopyWithProgress 1400-8.txt hdfs://master:9000/user/dengguoqing/1400-8.txt`
    
- 显示Hadoop文件系统中一组路径的文件信息

    `hadoop jar general-1.0-SNAPSHOT.jar org.guoqing.map.ListStatus hdfs://master:9000/user/dengguoqing hdfs://master:9000/user`