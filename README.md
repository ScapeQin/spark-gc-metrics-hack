Hackish tools for logging a whole lot of GC statistics from Spark and
logging them in a machine-readable JSON format after every GC.

Also, can tie in with a modified JVM that supports making "dummy" heap
allocations in an efficient way. This allows us to vary the
effective size of the eden space without "real" support for this.
