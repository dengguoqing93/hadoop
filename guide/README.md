#hadoop权威指南

##hadoop命令
- 列出文件系统中由哪些块构成：hdfs fsck / -files -blocks
- distcp并行复制：hadoop distcp file1 file2
- 从本地文件系统将一个文件辅助到HDFS:hadoop fs -copyFromLocal /input/docs/test.txt hdfs://localhost/user/tom/test.txt
- 将文件赋值到本地系统：hadoop -copyToLocal test.txt test.txt
- 在HDFS新建文件夹：hadoop fs -mkdir books


##YARN配置
- 队列层次结构如下
    ```
        root
         |——prod
         |——dev
             |——eng
             |——science
    ```
- 容量调度器基本配置文件
    ```
        <?xml version="1.0"?>
        <configuration>
            <property>
                <name>yarn.scheduler.capacity.root.queues</name>
                <value>prod,dev</value>
            </property>
        
            <property>
                <name>yarn.scheduler.capacity.root.dev.queues</name>
                <value>eng,science</value>
            </property>
        
            <property>
                <name>yarn.scheduler.capacity.root.prod.capacity</name>
                <value>40</value>
            </property>
        
        
            <property>
                <name>yarn.scheduler.capacity.root.dev.capacity</name>
                <value>60</value>
            </property>
        
            <property>
                <name>yarn.scheduler.capacity.root.dev.maximum-capacity</name>
                <value>75</value>
            </property>
        
        
            <property>
                <name>yarn.scheduler.capacity.root.dev.eng.capacity</name>
                <value>50</value>
            </property>
            
            <property>
                <name>yarn.scheduler.capacity.root.dev.science.capacity</name>
                <value>50</value>
            </property>
        </configuration> 
    ```

##Hadoop序列化接口
- Writable接口

        该接口定义了两个方法，一个将其状态写入DataOutput二进制流，另一个从DataInput二进制流读取状态

    + Text类型（类似于java的String类型）
    
            Text使用标准UTF-8编码。
            索引：由于Text使用UTF-8编码，因此Text类和Java String类之间存在一定的差别
            Unicode：
            迭代：利用字节偏移量实现的位置索引，对Text类中的Unicode字符进行迭代比较复杂，因为无法通过简单地增加索引值来实现该迭代。
            参考TextIterator类
            可变性：Text类是可变的
            可以调用toString方法将Text转化为String


##hadoop常用API
- FileSystem
    + 读取数据
    > public static FileSystem get(Configuration conf) throws IOException
      public static FileSystem get(URI uri,Configuration conf) throws IOException
      public static FileSystem get(URI uri,Configuration conf,String user)throws IOException
  
    + 写入数据
    > public FSDataOutputStream create(Path f)throws IOException
    (该方法多个重载版本，参考原方法或接口文档)     
      public FSDataOutputStream append(Path f)throws IOException
    
    + 目录
    > public boolean mkdirs(Path f)throws IOException 
    
    + 列出文件
    > public FileStatus[] listStatus(Path f) throws IOException
      public FileStatus[] listStatus(Path f,PathFilter filter) throws IOException
      public FileStatus[] listStatus(Path[] files) throws IOException
      public FileStatus[] listStatus(Path[] files,PathFilter filter) throws IOException
    
    + 文件模式
    > public FileStatus[] globStatus(Path PathPattern) throws IOException(该操作支持通配符)
      public FileStatus[] globStatus(Path pathPattern,PathFilter filter) throws IOException
      
    + PathFilter对象
    > 该对象是一个函数式接口，用来过滤路径
    