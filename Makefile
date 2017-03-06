# for icc run "make CC=icc" same for different compiler
# for cuda enabled run "make CUDA=1"

#VARIABLES
CUDA=0
PHI=0
DEBUG=0
WARNINGS=0
VTUNE=0
CC=g++
LD=$(CC)
AVX=0
SSE=0
NVCC=/usr/local/cuda/bin/nvcc

EXECUTABLE=rtree_test

CFLAGS= -c -fpermissive -std=c++0x -I../../../ -DLINUX
LDFLAGS= -lrt

DIR_STREAM=../../../common/stream
_SOURCES_STREAM=cCharStream.cpp cFileStream.cpp
SOURCES_STREAM = $(patsubst %,$(DIR_STREAM)/%,$(_SOURCES_STREAM))

DIR_CORE=../../../dstruct/paged/core
_SOURCES_CORE=cBucketHeader.cpp cBucketHeaderStorage.cpp cCacheStatistics.cpp cDStructHeader.cpp cMemoryPool.cpp cNode.cpp \
	cNodeHeader.cpp cQueryStatistics.cpp cQuickDB.cpp cTreeHeader.cpp cTreeNodeHeader.cpp cNodeCache.cpp
SOURCES_CORE = $(patsubst %,$(DIR_CORE)/%,$(_SOURCES_CORE))

DIR_RTREE=../../../dstruct/paged/rtree
_SOURCES_RTREE=cCommonRTree.cpp cRTreeHeader.cpp cRTreeLog.cpp cSignatureController.cpp cSignatureStorage.cpp cSignatureKey.cpp cSignatureRecord.cpp
SOURCES_RTREE = $(patsubst %,$(DIR_RTREE)/%,$(_SOURCES_RTREE))

DIR_QPROC=../../../dstruct/paged/queryprocessing
_SOURCES_QPROC=cQueryProcStat.cpp cRangeQueryConfig.cpp cRangeQueryContext.cpp 
SOURCES_QPROC = $(patsubst %,$(DIR_QPROC)/%,$(_SOURCES_QPROC))

DIR_COMPRESSION=../../../common/compression
_SOURCES_COMPRESSION=Coder.cpp cUniversalCoderEliasDelta.cpp cUniversalCoderEliasFibonacci.cpp cUniversalCoderFastEliasDelta.cpp cUniversalCoderFastEliasFibonacci.cpp \
	cUniversalCoderFastFibonacci2.cpp cUniversalCoderFastFibonacci3.cpp cUniversalCoderFibonacci2.cpp cUniversalCoderFibonacci3.cpp \
	cUniversalCoderFixedLength.cpp cCoderCommon.cpp
SOURCES_COMPRESSION = $(patsubst %,$(DIR_COMPRESSION)/%,$(_SOURCES_COMPRESSION))

DIR_COMMON=../../../common
_SOURCES_COMMON=cBitString.cpp cBitArray.cpp cNumber.cpp cMemory.cpp cSignature.cpp
SOURCES_COMMON = $(patsubst %,$(DIR_COMMON)/%,$(_SOURCES_COMMON))

DIR_UTILS=../../../common/utils
_SOURCES_UTILS=cTimer.cpp cCounter.cpp cHistogram.cpp
SOURCES_UTILS = $(patsubst %,$(DIR_UTILS)/%,$(_SOURCES_UTILS))

DIR_RANDOM=../../../common/random
_SOURCES_RANDOM=cGaussRandomGenerator.cpp cUniformRandomGenerator.cpp 
SOURCES_RANDOM = $(patsubst %,$(DIR_RANDOM)/%,$(_SOURCES_RANDOM))

DIR_DATATYPE=../../../common/datatype
_SOURCES_DATATYPE=cBasicType.cpp cDataType.cpp cDTDescriptor.cpp
SOURCES_DATATYPE = $(patsubst %,$(DIR_DATATYPE)/%,$(_SOURCES_DATATYPE))

DIR_TUPLE=../../../common/datatype/tuple
_SOURCES_TUPLE=cSpaceDescriptor.cpp cTuple.cpp cHNTuple.cpp cMbrSideSizeOrder.cpp
SOURCES_TUPLE = $(patsubst %,$(DIR_TUPLE)/%,$(_SOURCES_TUPLE))

DIR_MEMDATSTR=../../../common/memdatstruct
_SOURCES_MEMDATSTR=cMemoryManager.cpp cHeapMemoryBlock.cpp cBlockLinkedList.cpp
SOURCES_MEMDATSTR = $(patsubst %,$(DIR_MEMDATSTR)/%,$(_SOURCES_MEMDATSTR))

DIR_CUDA_NV=../../../dstruct/paged/cuda
_SOURCES_CUDA_NV= cCudaProcessor.cu  cCudaProcessor35.cu
SOURCES_CUDA_NV = $(patsubst %,$(DIR_CUDA_NV)/%,$(_SOURCES_CUDA_NV))

DIR_CUDA_CPP=../../../dstruct/paged/cuda
_SOURCES_CUDA_CPP= cGpuConst.cpp cCudaTimer.cpp cMemoryManagerCuda.cpp
SOURCES_CUDA_CPP = $(patsubst %,$(DIR_CUDA_CPP)/%,$(_SOURCES_CUDA_CPP))

SOURCES_MAIN=rtree.cpp

SOURCES=$(SOURCES_MAIN) $(SOURCES_CORE) $(SOURCES_RTREE) $(SOURCES_STREAM) $(SOURCES_COMPRESSION) $(SOURCES_DATATYPE) $(SOURCES_TUPLE) \
	$(SOURCES_RANDOM) $(SOURCES_MEMDATSTR) $(SOURCES_COMMON) $(SOURCES_UTILS) $(SOURCES_QPROC)

OBJECTS=$(SOURCES_MAIN:.cpp=.o) $(SOURCES_CORE:.cpp=.o) $(SOURCES_STREAM:.cpp=.o) $(SOURCES_COMPRESSION:.cpp=.o) $(SOURCES_DATATYPE:.cpp=.o) \
	$(SOURCES_TUPLE:.cpp=.o) $(SOURCES_RANDOM:.cpp=.o) $(SOURCES_MEMDATSTR:.cpp=.o) $(SOURCES_COMMON:.cpp=.o) $(SOURCES_UTILS:.cpp=.o) \
	$(SOURCES_RTREE:.cpp=.o) $(SOURCES_QPROC:.cpp=.o)

ifeq ($(DEBUG),1)
	CFLAGS += -g -O0
else
	#CFLAGS += -O2 #nefunguje range scan
endif

ifeq ($(CUDA),1)
	LD=$(NVCC)
	CFLAGS += -I/usr/local/cuda/include -DCUDA_ENABLED 
	NVCCFLAGS= -I../../../ -ccbin g++ -m64 -DCUDA_ENABLED -DLINUX -gencode arch=compute_35,code=sm_35 -dc
	#NVCCFLAGS += -maxrregcount=20
	LDFLAGS= -m64 -lcudart -lcudadevrt -ccbin $(CC) -gencode arch=compute_35,code=sm_35
	SOURCES += $(SOURCES_CUDA_NV) $(SOURCES_CUDA_CPP)
	OBJECTS += $(SOURCES_CUDA_NV:.cu=.o) $(SOURCES_CUDA_CPP:.cpp=.o) 
	ifeq ($(DEBUG),1)
		NVCCFLAGS += -G -g -O0
	endif	
endif

ifeq ($(PHI),1)
	export OFFLOAD_REPORT=2
	CC = icpc
	CFLAGS = -c -O0 -fpermissive -std=c++0x -I../../../ -DLINUX -openmp -DPHI_ENABLED
	LDFLAGS += -openmp -DPHI_ENABLED
endif

ifeq ($(WARNINGS),0)
	CFLAGS += -w
endif

ifeq ($(VTUNE),1)
	CC=icpc
	CFLAGS += -DVTUNE_CPP -I/data/development/intel/vtune_amplifier_xe_2015/include 
	#LDFLAGS += -DVTUNE_CPP -L/data/development/intel/vtune_amplifier_xe/lib64/ -littnotify
	LDFLAGS += -DVTUNE_CPP -I/data/development/intel/vtune_amplifier_xe_2015/include /data/development/intel/vtune_amplifier_xe_2015/lib64/libittnotify.a
endif

ifeq ($(SSE),1)
  CFLAGS += -DSSE_ENABLED -msse4.1
endif

ifeq ($(AVX),1)
  CFLAGS += -DAVX_ENABLED -mavx
endif

all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS) 
	$(LD) $(OBJECTS) $(LDFLAGS)  -o $@

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@

%.o : %.cu
	$(NVCC)  $(NVCCFLAGS) -o $@ -c $<
clean:
	rm -rf $(OBJECTS) $(EXECUTABLE)
run:
	./$(EXECUTABLE)
vtune:
	icpc $(SOURCES) -lrt -w -fpermissive -std=c++0x -I../../../ -DLINUX -g -I/data/development/intel/vtune_amplifier_xe_2015/include /data/development/intel/vtune_amplifier_xe_2015/lib64/libittnotify.a -lpthread -o rtree_test

