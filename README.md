### How to install Spark

- [How to use PySpark on your computer](https://towardsdatascience.com/how-to-use-pyspark-on-your-computer-9c7180075617)

![Alt Text](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*J6jWDf1eYu3U6SHvfAEt1A.jpeg)

#### TLDR

- Install Java by entering the following command: `brew install --cask adoptopenjdk`
- Confirm by `java -version`
- Download Spark from [here](https://spark.apache.org/downloads.html)
- Unzip file `tar -xzf spark-3.2.2-bin-hadoop3.tgz`
- Move to a location `mv spark-3.2.2-bin-hadoop3 /opt/spark-3.2.2`
- Create symlink `ln -s /opt/spark-2.3.0 /opt/spark̀`
- Export in .zshrc `export SPARK_HOME=/opt/spark`
- Export in .zshrc `export PATH=$SPARK_HOME/bin:$PATH`
- Install pyspark `pip install pyspark`
- Confirm by `pyspark`

### Resources

- [cheat sheet](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)
- [cheat sheet](https://runawayhorse001.github.io/LearningApacheSpark/cheat.html)
- [rdd vs dataframe vs dataset](https://phoenixnap.com/kb/rdd-vs-dataframe-vs-dataset#:~:text=DataFrames%20are%20a%20SparkSQL%20data,and%20the%20convenience%20of%20RDDs.)
- [websocket resources](https://github.com/ColinEberhardt/awesome-public-streaming-datasets)

### Questions

<details>
  <summary>Terminology</summary>
  <div>
    SparkSession is an entry point to Spark functionality and represents the connection to a Spark cluster.<br>
    It consists of a driver program and a set of executor programs, and it can run on a cluster of machines.<br>
    SparkSession was introduced in Spark 2.0 as a new unified entry point for working with structured data and the DataFrames API.<br>
    SparkSession is the entry point to using the Spark SQL API. It is responsible for creating DataFrames, executing SQL queries, and reading and writing data in a variety of formats.<br>
    SparkContext — provides connection to Spark with the ability to create RDDs<br>
    SQLContext — provides connection to Spark with the ability to run SQL queries on data<br><br>
    SparkSession — all-encompassing context which includes coverage for SparkContext, SQLContext and HiveContext.
</div>
</details>

<details>
  <summary>What is spark context</summary>
  <div>
    In Spark, a SparkContext represents the connection to a Spark cluster and can be used to create RDDs, accumulators, and broadcast variables on that cluster. It is the entry point to any Spark functionality and allows a Spark application to access Spark Cluster with the help of a Resource Manager.
    <br>
    In more technical terms, a SparkContext is a client-side driver that coordinates the execution of the job on a cluster. When a Spark application is executed, it connects to the cluster through the SparkContext and communicates with the Resource Manager to allocate and manage resources on the cluster. The SparkContext also sets up the execution environment for Spark and provides access to the configuration settings and Spark APIs.
</div>
</details>

<details>
<summary>Datasets vs DataFrames</summary>
<div>
    Datasets are strongly typed APIs, which means they allow users to specify the schema for their data and get compile-time type safety. DataFrames, on the other hand, are weakly typed APIs, which means they infer the schema of the data at runtime.<br>
    Datasets are faster than DataFrames because of the strong typing and compile-time optimization. However, this comes at the cost of more memory usage, since Datasets need to store the schema information.
</br>
</details>

<details>
<summary>Structured Streaming</summary>
<div>
    Structured streaming is a high-level API for stream processing provided by Apache Spark, a fast and general-purpose cluster-computing system. The primary aim of structured streaming is to make it easier to build end-to-end streaming applications, which integrate with storage and serving systems.

Here are a few core concepts around structured streaming:

1. **Unbounded Data**: In the context of structured streaming, data is considered as unbounded, i.e., it is a never-ending input data stream. This data could be consumed from various sources like Kafka, Flume, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like `map`, `reduce`, `join`, `window`, etc.

2. **DataFrame/Dataset API**: Structured Streaming uses DataFrame and Dataset APIs for handling data. Data is processed in Spark using data frames or data sets, where each data item is a row that is processed as it comes in.

3. **Streaming Computation**: A streaming computation is treated as a continual incrementally updated query on the unbounded input data. The system automatically breaks down the streaming computation into discrete stages that can be computed and materialized in a distributed and fault-tolerant manner.

4. **Output Modes**: There are three types of output modes in Structured Streaming: complete mode, append mode, and update mode. The output mode defines what will be written to the output sink when there is new data available in the DataFrame/Dataset.

   - **Complete Mode**: The entire updated DataFrame will be written to the sink.
   - **Append Mode**: Only the new rows appended to the DataFrame will be written to the sink.
   - **Update Mode**: Only the rows that were updated in the DataFrame will be written to the sink.

5. **Event Time and Watermarking**: Structured Streaming provides support for event time, which is the time embedded in the data itself, and watermarks, which are a threshold of how late the data is expected to be.

6. **Fault Tolerance**: Structured streaming ensures end-to-end, exactly-once fault-tolerance through checkpointing and Write-Ahead Logs, which allow it to recover state and continue processing after failures.

Understanding the core concept of structured streaming will help you design applications that can handle live data streams efficiently and effectively.
</br>

</details>
