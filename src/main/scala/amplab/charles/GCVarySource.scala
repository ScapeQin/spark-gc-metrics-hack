package amplab.charles

import com.codahale.metrics.MetricRegistry

import com.codahale.metrics.{Gauge, Metric, MetricSet}

import org.apache.spark.charles.SourceWrapper

import java.util.{Map => JMap, HashMap => JHashMap, Collections => JCollections}

class GCVarySource extends SourceWrapper {
    val sourceName = "gcVary"
    val metricRegistry = new MetricRegistry()
    
    GCVarySampler.setupGCNotifications()

    metricRegistry.registerAll(new GCVarySet())
}
