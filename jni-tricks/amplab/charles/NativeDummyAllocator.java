package amplab.charles;

public final class NativeDummyAllocator implements DummyArrayAllocator {
    @Override
    public long[] allocateLongArray(int size) {
        System.err.println("NativeDummyAllocator: allocate " + size);
        return JniTricks.allocateUninitializedLongArray(size);
    }
}
