CC = g++
CFLAGS = -std=c++11 -O3 -Wall -fPIC -pedantic -lpthread
LINK = g++
LFLAGS = ${CFLAGS}

all: libdram_reassignment.so

%.o : %.cpp
	$(CC) $(CFLAGS) -c $< -o $@
	
libdram_reassignment.so: dram_reassignment.o
	${LINK} ${LFLAGS} -shared -O3 -fPIC -o $@ $^
