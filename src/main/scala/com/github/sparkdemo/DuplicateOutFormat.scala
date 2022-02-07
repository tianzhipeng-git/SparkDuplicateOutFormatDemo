package com.github.sparkdemo

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

import java.io.Serializable

/**
 * Very Dangerous Toy Code. DO NOT USE IN PRODUCTION.
 */
class DuplicateOutFormat
  extends FileFormat
    with DataSourceRegister
    with Serializable {
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException()
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val format1 = options("format1")
    val format2 = options("format2")
    val format1Instance = DataSource.lookupDataSource(format1, sparkSession.sessionState.conf)
      .newInstance().asInstanceOf[FileFormat]
    val format2Instance = DataSource.lookupDataSource(format2, sparkSession.sessionState.conf)
      .newInstance().asInstanceOf[FileFormat]

    val writerFactory1 = format1Instance.prepareWrite(sparkSession, job, options, dataSchema)
    val writerFactory2 = format2Instance.prepareWrite(sparkSession, job, options, dataSchema)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ".dup"

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        val path1 = path.replace(".dup", writerFactory1.getFileExtension(context))
        val path2 = path.replace(".dup", writerFactory2.getFileExtension(context))
        val writer1 = writerFactory1.newInstance(path1, dataSchema, context)
        val writer2 = writerFactory2.newInstance(path2, dataSchema, context)
        new OutputWriter {
          override def write(row: InternalRow): Unit = {
            writer1.write(row)
            writer2.write(row)
          }

          override def close(): Unit = {
            writer1.close()
            writer2.close()
          }
        }
      }
    }
  }

  override def shortName(): String = "dup"
}
