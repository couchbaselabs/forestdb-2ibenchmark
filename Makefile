CC=g++
override CFLAGS := -g -Werror -Iutils/

FDBFLAGS=-I$(FDBDIR)/include -L$(FDBDIR)/build

IDIR=utils
_SOURCES = iniparser.cc strgen.cc xxhash.c
SOURCES = $(patsubst %,$(IDIR)/%,$(_SOURCES))

LIBS=-lpthread -lforestdb

.DEFAULT_GOAL := forestdb_standalone_test

forestdb_standalone_test: $(OBJ)
	$(CC) -o $@ forestdb_workload.cc $(SOURCES) $^ $(CFLAGS) $(LIBS) $(FDBFLAGS)

.PHONY: clean

clean:
	rm -f incrementalsecondary.txt forestdb_standalone_test
	rm -rf data/
	mkdir -p data

