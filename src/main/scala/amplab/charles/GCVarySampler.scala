package amplab.charles

import java.util.Random

import java.lang.management.ManagementFactory
import javax.management.openmbean.CompositeData
import javax.management.{Notification, NotificationListener, NotificationEmitter}
import com.sun.management.GarbageCollectionNotificationInfo

import scala.collection.JavaConverters._

import java.util.{Map => JMap, HashMap => JHashMap, Collections => JCollections}

import org.apache.spark.charles.GCTriggeredSink

object GCVarySampler extends NotificationListener {
    var metricsSink: GCTriggeredSink = null
    @volatile var lastExtraSize = 0L
    @volatile var lastExtraTime = 0L
    @volatile var theArray: Array[Long] = null
    var alreadySetup = false
    private val r = new Random(42)

    def findYoungSize: Long = {
        for (pool <- ManagementFactory.getMemoryPoolMXBeans.asScala) {
            if (pool.getName == "PS Eden Space") {
                return pool.getUsage.getMax
            }
        }
        throw new Error("Could not get Eden capacity")
    }

    // Native method we believe won't be optimized across
    // We depend on this not being one of the native functions which are
    // special-cased by the JIT. (See opto/library_call.cpp)
    def _nativeMystery(): Unit = {
        Runtime.getRuntime.availableProcessors
    }

    val maxYoungSize = findYoungSize / 8L

    private def allocateDummy(size: Int) {
        lastExtraSize = size
        val startTime = System.nanoTime
        /*
        val CHUNK = 1024 * 1024 * 8
        for (i <- 0 to (size / CHUNK)) {
            theArray = new Array[Long](CHUNK)
            _nativeMystery()
            theArray = null
        }
        */
        theArray = new Array[Long](size)
        _nativeMystery()
        theArray = null
        val endTime = System.nanoTime
        lastExtraTime = endTime - startTime
    }

    private def sampleSize(): Int = {
        return r.nextInt(maxYoungSize.asInstanceOf[Int])
    }

    def setupGCNotifications() {
        assert(!alreadySetup)
        alreadySetup = true
        for (gc <- ManagementFactory.getGarbageCollectorMXBeans.asScala) {
            gc.asInstanceOf[NotificationEmitter].addNotificationListener(
                this, null, null)
        }
    }

    override def handleNotification(notification: Notification,
                                    handback: AnyRef) {
        if (notification.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION) {
            val info = GarbageCollectionNotificationInfo.from(
                notification.getUserData.asInstanceOf[CompositeData]
            )

            if (info.getGcAction() == "end of minor GC") {
                if (metricsSink != null) {
                    metricsSink.report()
                }
                allocateDummy(sampleSize())
            }
        }
    }
}
