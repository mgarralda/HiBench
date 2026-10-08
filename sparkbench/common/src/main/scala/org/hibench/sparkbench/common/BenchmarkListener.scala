package org.hibench.sparkbench.common

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart}

/** Structured ID emitted even when the Spark logger is set to ERROR. */
class BenchmarkListener extends SparkListener {
  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    event.appId.foreach(id => println(s"HIBENCH_APPLICATION_ID=$id"))
  }
}
