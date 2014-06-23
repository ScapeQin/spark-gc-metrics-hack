package amplab.charles


import com.codahale.metrics.MetricRegistry
import org.apache.spark.metrics.source.Source

import com.codahale.metrics.{Gauge, Metric, MetricSet}

import java.util.{Map => JMap, HashMap => JHashMap, Collections => JCollections}

class GCVarySource extends Source {
    val sourceName = "gcVary"
    val metricRegistry = new MetricRegistry()
    
    GCVarySampler.setupGCNotifications()

    object GCVarySet extends MetricSet {
        override def getMetrics: JMap[String, Metric] = {
            val result = new JHashMap[String, Metric]
            result.put("lastExtraSize", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.lastExtraSize
            })
            return JCollections.unmodifiableMap(result)
        }
    }

    metricRegistry.registerAll(GCVarySet)
}
