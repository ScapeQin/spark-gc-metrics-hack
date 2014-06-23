package amplab.charles

import com.codahale.metrics.{Gauge, Metric, MetricSet}

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._

import java.util.{Map => JMap, HashMap => JHashMap, Collections => JCollections}

class JmxSet extends MetricSet {
    override def getMetrics: JMap[String, Metric] = {
        val result = new JHashMap[String, Metric]
        for (pool <- ManagementFactory.getMemoryPoolMXBeans.asScala) {
            val name = pool.getName.replaceAll(" ", "-")
            result.put("pool." + name + ".postUsed", new Gauge[Long] {
                override def getValue: Long = pool.getCollectionUsage.getUsed
            })
            result.put("pool." + name + ".postMax", new Gauge[Long] {
                override def getValue: Long = pool.getCollectionUsage.getMax
            })
        }
        return JCollections.unmodifiableMap(result)
    }
}
