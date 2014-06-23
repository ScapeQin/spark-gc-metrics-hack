package org.apache.spark.charles

import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule

import java.util.Properties

import org.apache.spark.metrics.sink.Sink
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.SecurityManager

import java.io.FileOutputStream

import com.fasterxml.jackson.databind.ObjectMapper

import java.util.concurrent.TimeUnit

import amplab.charles.GCVarySampler

class GCTriggeredSink(val properties: Properties, val registry: MetricRegistry, val secManager: SecurityManager) extends Sink {
  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, true))

  GCVarySampler.metricsSink = this

  val outputFile = Option(properties.getProperty("file")) match {
    case Some(file) => file
    case None => "./gc-metrics-json"
  }

  var outputStream = new FileOutputStream(outputFile)
  val generator = mapper.getFactory().createGenerator(outputStream)

  def report(): Unit = {
    synchronized {
      if (outputStream != null) {
        outputStream.write((System.currentTimeMillis + ": ").getBytes())
        mapper.writeValue(generator, registry)
        generator.flush()
        outputStream.write('\n'.asInstanceOf[Int])
      }
    }
  }

  override def start() {
  }

  override def stop() {
    synchronized {
      generator.close()
      outputStream.close()
      outputStream = null
    }
  }
}
