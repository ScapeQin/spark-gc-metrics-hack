package amplab.charles

import java.util.Random
import java.util.{Timer, TimerTask}

import java.lang.management.ManagementFactory
import javax.management.openmbean.CompositeData
import javax.management.{Notification, NotificationListener, NotificationEmitter}
import com.sun.management.GarbageCollectionNotificationInfo

import scala.collection.JavaConverters._

import java.util.{Map => JMap, HashMap => JHashMap, Collections => JCollections}

import org.apache.spark.charles.GCTriggeredSink

object GCVarySampler extends NotificationListener {
    var metricsSink: GenericSink = null
    @volatile var disableSampling = false
    @volatile var lastExtraSize = 0L
    @volatile var totalExtraSize = 0L
    @volatile var lastExtraTime = 0L
    @volatile var haveNativeAllocator = false
    @volatile var numAllocations = 0L
    @volatile var theArray: Array[Long] = null
    @volatile var heapDumpInterval = -1L
    @volatile var lastHeapDump = 0L
    @volatile var forceFullAfter = 0
    @volatile var sinceForceFull = 0
    @volatile var nextForceFull = 0
    @volatile var sameSampleRange = 4
    @volatile var forceFullPoisson = false
    @volatile var sampleHalf = false
    var alreadySetup = false
    private val r = new Random(42)
    private var heapDumpTimer: Timer = null

    disableSampling = System.getProperty(
        "amplab.charles.gcVary.disable", "false") == "true"

    heapDumpInterval = System.getProperty(
        "amplab.charles.gcVary.heapDumpInterval", "-1").toLong

    forceFullAfter = System.getProperty(
        "amplab.charles.gcVary.forceFullAfter", "0").toInt

    forceFullPoisson = System.getProperty(
        "amplab.charles.gcVary.forceFullPoisson", "true") == "true"

    sampleHalf = System.getProperty(
        "amplab.charles.gcVary.sampleHalf", "false") == "true"

    private def getAllocator(): DummyArrayAllocator = {
        try {
            val nativeCls = Class.forName(
                "amplab.charles.NativeDummyAllocator"
            )
            System.err.println("GCVarySampler: Using native allocator trick")
            System.out.println("GCVarySampler: Using native allocator trick")
            haveNativeAllocator = true
            return nativeCls.newInstance().asInstanceOf[DummyArrayAllocator]
        } catch {
            case cnfe: ClassNotFoundException => 
                System.err.println("GCVarySampler: no native allocator");
                haveNativeAllocator = false
                return new SimpleDummyArrayAllocator()
        }
    }

    private val allocator = getAllocator()

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

    private def allocateDummy(_size: Int) {
        var size = _size
        if (size < 0) {
            size = lastExtraSize.asInstanceOf[Int]
        } else {
            lastExtraSize = size
        }
        totalExtraSize += size
        val startTime = System.nanoTime
        /*
        val CHUNK = 1024 * 1024 * 8
        for (i <- 0 to (size / CHUNK)) {
            theArray = new Array[Long](CHUNK)
            _nativeMystery()
            theArray = null
        }
        */
        System.err.println("About to allocator.allocateLongArray: " +
                           allocator + " " + size)
        try {
            theArray = allocator.allocateLongArray(size)
        } catch {
            case t: Throwable => 
                System.err.println("GCVarySampler: Error in allocator: " + t)
                throw t
        }
        System.err.println("Done allocator.allocateLongArray: " + theArray)
        numAllocations += 1
        _nativeMystery()
        theArray = null
        val endTime = System.nanoTime
        lastExtraTime = endTime - startTime
    }

    private def sampleSize(): Int = {
        if (!sampleHalf) {
            System.err.println("NO SAMPLE HALF")
            return r.nextInt(maxYoungSize.asInstanceOf[Int])
        } else {
            return maxYoungSize.asInstanceOf[Int] / 2 + r.nextInt(maxYoungSize.asInstanceOf[Int] / 2)
        }
    }

    def setupGCNotifications() {
        System.err.println("In setupGCNotifications")
        assert(!alreadySetup)
        alreadySetup = true
        for (gc <- ManagementFactory.getGarbageCollectorMXBeans.asScala) {
            gc.asInstanceOf[NotificationEmitter].addNotificationListener(
                this, null, null)
        }
        System.err.println("Out setupGCNotifications")

        if (heapDumpInterval > 0L) {
            heapDumpTimer = new Timer("GCVarySampler heap dump timer", true)
            heapDumpTimer.scheduleAtFixedRate(new TimerTask {
                override def run: Unit = {
                    System.err.println("Producing heap histogram")
                    val filename = "heapHistogram-" + System.currentTimeMillis
                    HeapHistogram.selfHistogramToFile(filename)
                    lastHeapDump = System.currentTimeMillis
                }
            }, 300L * 1000L, heapDumpInterval * 1000L)
            System.err.println("Setup heap histogram taker...")
        }
    }

    private def generatePoisson(mean: Int): Int = {
        val L = Math.exp(-mean)
        var k = 0
        var p = 1.0
        while (p > L) {
            k += 1
            val u = r.nextDouble
            p *= u
        }
        return k - 1
    }

    override def handleNotification(notification: Notification,
                                    handback: AnyRef) {
        System.err.println("In handleNotification " + notification);
        if (notification.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION) {
            val info = GarbageCollectionNotificationInfo.from(
                notification.getUserData.asInstanceOf[CompositeData]
            )
            if (metricsSink != null) {
                metricsSink.report()
            }
            if (info.getGcAction() == "end of minor GC") {
                if (!disableSampling) {
                    if (forceFullAfter > 0 && 
                        (sinceForceFull < sameSampleRange ||
                         sinceForceFull >= forceFullAfter - sameSampleRange)) {
                        allocateDummy(-1)
                    } else {
                        allocateDummy(sampleSize())
                    }
                }
                if (forceFullAfter > 0) {
                    sinceForceFull += 1
                    if (sinceForceFull >= forceFullAfter) {
                        sinceForceFull = 0
                        if (forceFullPoisson) {
                            nextForceFull = generatePoisson(forceFullAfter)
                        } else {
                            nextForceFull = forceFullAfter
                        }
                        System.gc()
                    }
                }
            }
        }
    }
}
