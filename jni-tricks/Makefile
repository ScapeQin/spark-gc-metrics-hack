JDK=/usr/lib/jvm/java-7-openjdk-amd64
JDK_SOURCE=/scratch/charles/openjdk-build/jdk8
JDK_SOURCE_GENERATED=/scratch/charles/openjdk-build/jdk8/build/linux-x86_64-normal-server-release/hotspot/linux_amd64_compiler2/generated

LIBNAME=acJniTricks
SOURCES=amplab_charles_JniTricks.c
JAVA_SOURCES=amplab/charles/JniTricks.java \
	     amplab/charles/NativeDummyAllocator.java

# Name of jar file that needs to be created
JARFILE=acJniTricks.jar

CXX=g++
LINK.c=g++

COMMON_FLAGS=-fno-strict-aliasing -fPIC -std=c99
COMMON_FLAGS+= -W -Wall  -Wno-unused -Wno-parentheses -fstack-protector-all 
CFLAGS=-ggdb -gstabs+ -O2 -fno-omit-frame-pointer -fPIC
CXXFLAGS=-ggdb -gstabs+ -O2 -fno-omit-frame-pointer -fPIC
# XXX: We really should figuer out where
CPPFLAGS=-DTARGET_COMPILER_gcc -DLINUX -DTARGET_OS_FAMILY_linux -DTARGET_ARCH_x86 -DCOMPILER2
CPPFLAGS+= -DVM_LITTLE_ENDIAN -D_LP64=1 -D_REENTRANT -DDONT_USE_PRECOMPILED_HEADER -DAMD64 -D_GNU_SOURCE
CPPFLAGS+= -DTARGET_OS_ARCH_linux_x86
OBJECTS=$(SOURCES:%.c=%.o)
LIBRARY=lib$(LIBNAME).so
LDFLAGS=-Wl,-soname=$(LIBRARY) -static-libgcc
LIBARCH=amd64
LIBJDK=-L $(JDK)/jre/lib/$(LIBARCH)/server -ljvm
LIBRARIES=-L . -L $(JDK)/jre/lib/$(LIBARCH) -lc
LINK_SHARED=$(LINK.c) -shared -o $@ $(LDFLAGS)

OSNAME=linux
CPU=x86

# Common -I options
CPPFLAGS += -I.
CPPFLAGS += -I$(JDK)/include -I$(JDK)/include/$(OSNAME)
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/share/vm
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/share/vm/precompiled
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/os/$(OSNAME)/vm
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/os/posix/vm
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/cpu/$(CPU)/vm
CPPFLAGS += -I$(JDK_SOURCE)/hotspot/src/os_cpu/$(OSNAME)_$(CPU)/vm
CPPFLAGS += -I$(JDK_SOURCE_GENERATED)

all: $(LIBRARY) $(JARFILE) 

amplab_charles_JniTricks.o: amplab_charles_JniTricks.cc amplab_charles_JniTricks.h

# Build native library
$(LIBRARY): $(OBJECTS)
	$(LINK_SHARED) -v $(LIBJDK) $(OBJECTS) $(LIBRARIES)

# Build jar file
$(JARFILE): $(JAVA_SOURCES)
	rm -f -r classes
	mkdir -p classes
	$(JDK)/bin/javac -cp ../target/extra-spark-metrics-0.0-SNAPSHOT.jar \
	    -d classes $(JAVA_SOURCES)
	(cd classes; $(JDK)/bin/jar cf ../$@ *)

# Cleanup the built bits
clean:
	rm -f -r classes
	rm -f $(LIBRARY) $(JARFILE) $(OBJECTS)

