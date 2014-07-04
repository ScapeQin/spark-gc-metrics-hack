package amplab.charles

trait DummyArrayAllocator {
    def allocateLongArray(size: Int): Array[Long]
}

final class SimpleDummyArrayAllocator extends DummyArrayAllocator {
    def allocateLongArray(size: Int): Array[Long] = new Array[Long](size)
}
