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
            result.put("totalExtraSize", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.totalExtraSize
            })
            result.put("lastExtraTime", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.lastExtraTime
            })
            result.put("disableSampling", new Gauge[Boolean] {
                override def getValue: Boolean = GCVarySampler.disableSampling
            })
            result.put("heapDumpInterval", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.heapDumpInterval
            })
            result.put("lastHeapDump", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.lastHeapDump
            })
            result.put("forceFullAfter", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.forceFullAfter
            })
            result.put("sinceForceFull", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.sinceForceFull
            })
            result.put("numAllocations", new Gauge[Long] {
                override def getValue: Long = GCVarySampler.numAllocations
            })
            result.put("sampleHalf", new Gauge[Boolean] {
                override def getValue: Boolean = GCVarySampler.sampleHalf
            })
            result.put("haveNativeAllocator", new Gauge[Boolean] {
                override def getValue: Boolean = GCVarySampler.haveNativeAllocator
            })
            return JCollections.unmodifiableMap(result)
        }
    }

    metricRegistry.registerAll(GCVarySet)
}
