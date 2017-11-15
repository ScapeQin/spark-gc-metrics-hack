package amplab.charles;

import java.util.Random;

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
    public static native void setupSamplingPolicy(long minSize, long maxSize);

    /* For testing */
    public static void main(String[] args) {
        if (args[0].equals("test-sampling-policy")) {
            setupSamplingPolicy(0, 1024 * 1024);
            Random r = new Random();
            byte[] stuff;
            byte[] otherStuff = new byte[1024];
            for (int i = 0; i < 1000 * 1000 * 1000; ++i) {
                stuff = new byte[r.nextInt(1000 * 2)];
                if (r.nextInt(8) == 0) {
                    otherStuff = stuff;
                }
            }
            System.out.println("Done");
            System.out.println("otherStuff has " + otherStuff.length);
        } else {
            System.out.println("args[0] = " + args[0]);
        }
    }
}
