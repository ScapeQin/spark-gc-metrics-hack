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
    @volatile var extraArray: Array[Array[Long]] = new Array[Array[Long]](16)
    @volatile var heapDumpInterval = -1L
    @volatile var lastHeapDump = 0L
    @volatile var forceFullAfter = 0
    @volatile var sinceForceFull = 0
    @volatile var nextForceFull = 0
    @volatile var sameSampleRange = 4
    @volatile var forceFullPoisson = false
    @volatile var sampleHalf = false
    @volatile var sampleEmbedded = false
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

    sampleEmbedded = System.getProperty(
        "amplab.charles.gcVary.sampleEmbedded", "false") == "true"

    if (sampleEmbedded) {
        disableSampling = true
    }

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
                System.err.println("GCVarySampler: no native allocator: cnfe = " + cnfe);
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

    private def allocateDummy(_size: Long) {
        var size = _size
        if (size < 0) {
            size = lastExtraSize.asInstanceOf[Int]
        } else {
            lastExtraSize = size
        }
        totalExtraSize += size
        val startTime = System.nanoTime
        val MAX_SIZE = 2L * 1024L * 1024L * 1024L
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
        var i = 0
        try {
            while (size > MAX_SIZE) {
                extraArray(i) = allocator.allocateLongArray(MAX_SIZE.toInt)
                size -= MAX_SIZE
                i += 1
                System.err.println("Allocating extra array")
            }
            theArray = allocator.allocateLongArray(size.toInt)
        } catch {
            case t: Throwable => 
                System.err.println("GCVarySampler: Error in allocator: " + t)
                throw t
        }
        System.err.println("Done allocator.allocateLongArray: " + theArray)
        numAllocations += 1
        _nativeMystery()
        theArray = null
        while (i > 0) {
            extraArray(i) = null
            i -= 1
        }
        val endTime = System.nanoTime
        lastExtraTime = endTime - startTime
    }

    private def sampleSize(): Long = {
        if (!sampleHalf) {
            System.err.println("NO SAMPLE HALF")
            return r.nextLong() % maxYoungSize
        } else {
            return maxYoungSize / 2L + r.nextLong() % (maxYoungSize.asInstanceOf[Int] / 2L)
        }
    }

    def setupGCNotifications() {
        System.err.println("GCVarySampler: In setupGCNotifications: alreadySetup=" + alreadySetup + "; " +
                           "disableSampling=" + disableSampling)
        assert(!alreadySetup)
        for (gc <- ManagementFactory.getGarbageCollectorMXBeans.asScala) {
            gc.asInstanceOf[NotificationEmitter].addNotificationListener(
                this, null, null)
        }

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

        if (sampleEmbedded) {
            disableSampling = true
            System.err.println("GCVarySampler: sampleEmbedded=true")
            try {
                val jniTricksClass = Class.forName("amplab.charles.JniTricks")
                val setupSamplingPolicyMethod = jniTricksClass.getMethod("setupSamplingPolicy", classOf[Long], classOf[Long])
                val youngSize = findYoungSize
                setupSamplingPolicyMethod.invoke(null, 0L.asInstanceOf[java.lang.Long], youngSize.asInstanceOf[java.lang.Long])
            } catch {
                case (t: java.lang.Throwable) =>
                    System.err.println("GCVarySampler: problem calling setupSamplingPolicy: " + t)
                    t.printStackTrace()
            }
        } else {
            System.err.println("GCVarySampler: sampleEmbedded=false")
        }
        System.err.println("GCVarySampler: Out setupGCNotifications")
        alreadySetup = true
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
                        allocateDummy(-1L)
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
