package amplab.charles;

public class JniTricks {
    static {
        System.err.println("About to load acJniTricks; LD_LIBRARY_PATH="
                    + System.getenv("LD_LIBRARY_PATH"));
        try {
            System.loadLibrary("jvm");
            System.loadLibrary("acJniTricks");
        } catch (UnsatisfiedLinkError ule) {
            System.err.println(ule);
            ule.printStackTrace();
            Runtime.getRuntime().halt(100);
        }
        System.err.println("Loaded acJniTricks");
    }
    public static native long[] allocateUninitializedLongArray(int size);
}
