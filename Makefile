MODULE=hashdb
DEBUG=1
#OPTIMIZE=1
#PROFILE=1
#USE_GC=1
#LEAK_DETECT=1
#USE_READLINE=1
#USE_EDITLINE=1
#VALGRIND=1

MAJOR=1
MINOR=0

ifndef CXX
CXX = g++
endif

ifndef PREFIX
PREFIX=/usr/local
endif

.PHONY: all install

OS_TYPE = $(shell uname -s | \
  awk '{ split($$1,a,"_"); printf("%s", a[1]);  }')
OS_VERSION = $(shell uname -r | \
  awk '{ split($$1,a,"."); sub("V","",a[1]); \
  printf("%d%d%d",a[1],a[2],a[3]); }')
ARCH = $(shell uname -m)
ifeq ($(ARCH),i386)
  ARCH = x86
endif
ifeq ($(ARCH),i486)
  ARCH = x86
endif
ifeq ($(ARCH),i586)
  ARCH = x86
endif
ifeq ($(ARCH),i686)
  ARCH = x86
endif

ifeq ($(ARCH),x86)
ifneq ($(OS_TYPE),Darwin)
# Darwin lies
  CFLAGS += -DHAS_32BIT=1
endif
endif

ifeq ($(OS_TYPE),Darwin)
  AR_FLAGS = crvs
else
  AR_FLAGS = crv
endif

ifeq ($(OS_TYPE),CYGWIN)
#GC_CFLAGS += -L/usr/local/lib
else
ifeq ($(OS_TYPE),Darwin)
GC_CFLAGS += -I/usr/local/include
else
GC_CFLAGS += -I/usr/local/include
LIBS += -lrt -lpthread
endif
endif

ifdef USE_GC
CFLAGS += -DUSE_GC ${GC_CFLAGS}
LIBS += -lgc
endif
ifdef LEAK_DETECT
CFLAGS += -DLEAK_DETECT  ${GC_CFLAGS}
LIBS += -lleak
endif

ifdef USE_READLINE
ifeq ($(OS_TYPE),Linux)
  CFLAGS += -DUSE_READLINE
  LIBS += -lreadline
endif
ifeq ($(OS_TYPE),CYGWIN)
  CFLAGS += -DUSE_READLINE
  LIBS += -lreadline
endif
endif
ifdef USE_EDITLINE
ifeq ($(OS_TYPE),Linux)
  CFLAGS += -DUSE_EDITLINE
  LIBS += -leditline
endif
ifeq ($(OS_TYPE),CYGWIN)
  CFLAGS += -DUSE_EDITLINE
  LIBS += -ledit -ltermcap
endif
endif

BUILD_VERSION = $(shell git show-ref 2> /dev/null | head -1 | cut -d ' ' -f 1)
VERSIONCFLAGS += -DMAJOR_VERSION=$(MAJOR) -DMINOR_VERSION=$(MINOR) -DBUILD_VERSION=\"$(BUILD_VERSION)\"

CFLAGS += -std=c++20

CFLAGS += -Wall -Wno-strict-aliasing
# debug flags
ifdef DEBUG
CFLAGS += -g -DDEBUG=1
endif
# optimized flags
ifdef OPTIMIZE
CFLAGS += -O3 -march=native
endif
ifdef PROFILE
CFLAGS += -pg
endif
ifdef VALGRIND
CFLAGS += -DVALGRIND_TEST
endif

CPPFLAGS += $(CFLAGS)

LIBS += -lm

AUX_FILES = $(MODULE)/Makefile $(MODULE)/LICENSE $(MODULE)/README

LIB_SRCS = hashdb.cc prime.cc slice.cc gen.cc
LIB_CSRCS = blake3.c blake3_portable.c blake3_dispatch.c
LIB_OBJS = $(LIB_SRCS:%.cc=%.o) $(LIB_CSRCS:%.c=%.o)

TEST_LIB_SRCS = test.cc
TEST_LIB_OBJS = $(TEST_LIB_SRCS:%.cc=%.o)

EXECUTABLE_FILES =
LIBRARY = lib$(MODULE).a
INSTALL_LIBRARIES = lib$(MODULE).a
INCLUDES =
MANPAGES = lib$(MODULE).1

ifeq ($(OS_TYPE),CYGWIN)
EXECUTABLES = $(EXECUTABLE_FILES:%=%.exe)
TEST_EXEC = test_$(MODULE).exe
else
EXECUTABLES = $(EXECUTABLE_FILES)
TEST_EXEC = test_$(MODULE)
endif

ALL_SRCS = $(LIB_SRCS) $(LIB_SRCS) $(TEST_LIB_SRCS)
DEPEND_SRCS = $(ALL_SRCS)

all: $(LIBRARY) test LICENSE.i COPYRIGHT.i

version:
	@echo $(MODULE) $(MAJOR).$(MINOR).$(BUILD_VERSION) '('$(OS_TYPE) $(OS_VERSION)')' $(CFLAGS)

version.o: version.cc
	$(CXX) $(CFLAGS) $(VERSIONCFLAGS) -c version.cc

%.o: %.c
	$(CXX) $(CFLAGS) -c $< -o $@

$(LIBRARY):  $(LIB_OBJS)
	ar $(AR_FLAGS) $@ $^

$(TEST_EXEC): test.o $(LIB_OBJS)
	$(CXX) $(CFLAGS) -DTEST_LIB=1 test.o $(LDFLAGS) $(LIB_OBJS) -o $@ $(LIBS)

LICENSE.i: LICENSE
	rm -f LICENSE.i
	cat $< | sed s/\"/\\\\\"/g | sed s/\^/\"/g | sed s/$$/\\\\n\"/g | sed 's/%/%%/g' > $@

COPYRIGHT.i: LICENSE
	rm -f COPYRIGHT.i
	head -1 LICENSE | sed s/\"/\\\\\"/g | sed s/\^/\"/g | sed s/$$/\\\\n\"/g > $@

test: $(TEST_EXEC)
	./$(TEST_EXEC)

clean:
	\rm -f *.o core *.core *.gmon LICENSE.i COPYRIGHT.i $(EXECUTABLES) $(TEST_EXEC)

realclean: clean
	\rm -f *.a *.orig *.rej svn-commit.tmp

depend:
	./mkdep $(CFLAGS) $(DEPEND_SRCS)

version.o: Makefile

# DO NOT DELETE THIS LINE -- mkdep uses it.
# DO NOT PUT ANYTHING AFTER THIS LINE, IT WILL GO AWAY.

test.o: test.cc

# IF YOU PUT ANYTHING HERE IT WILL GO AWAY
