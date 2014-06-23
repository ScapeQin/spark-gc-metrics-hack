package amplab.charles

import com.codahale.metrics.MetricRegistry
import org.apache.spark.metrics.source.Source

class ExtraJvmSource extends Source {
    val sourceName = "extraJvm"
    val metricRegistry = new MetricRegistry()

    val jvmStatSet = new JvmStatSet()
    val jmxSet = new JmxSet() 

    metricRegistry.registerAll(jvmStatSet)
    metricRegistry.registerAll(jmxSet)
}
