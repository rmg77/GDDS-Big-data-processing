package org.apress.prohadoop.c3

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.mapred.Mapper
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reducer
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import scala.collection.Iterator
import scala.math.Ordering.String
import scala.reflect.internal.Reporter
import scala.reflect.io.Path
import scala.sys.process.processInternal.IOException
import scala.util.control.Exception


object WordCountOldAPI {

  class MyMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable],
            reporter: Reporter): Unit = {
      output.collect(new Text(value.toString()), new IntWritable(1))
    }
  }

  class MyReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    @throws[IOException]
    def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable],
               reporter: Reporter): Unit = {
      var sum = 0
      while (values.hasNext) sum += values.next.get
      output.collect(key, new IntWritable(sum))
    }
  }

  @throws[Exception]
  def main(args: Array[Ordering.String.type]) = {
    val conf = new JobConf(classOf[WordCountOldAPI])
    conf.setJobName("wordcount")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[WordCountOldAPI.MyMapper])
    conf.setCombinerClass(classOf[WordCountOldAPI.MyReducer])
    conf.setReducerClass(classOf[WordCountOldAPI.MyReducer])
    conf.setNumReduceTasks(1)
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat])
    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
  }
}
