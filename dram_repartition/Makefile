CC = g++
CFLAGS = -std=c++11 -O3 -Wall -fPIC -pedantic -lpthread
LINK = g++
LFLAGS = ${CFLAGS}

all: libdram_repartition.so

%.o : %.cpp
	$(CC) $(CFLAGS) -c $< -o $@
	
libdram_repartition.so: dram_repartition.o
	${LINK} ${LFLAGS} -shared -O3 -fPIC -o $@ $^
