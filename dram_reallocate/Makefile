
CC = g++
CFLAGS = -std=c++11 -O3 -Wall -fPIC -pedantic -lpthread
LINK = g++
LFLAGS = ${CFLAGS}
#OBJECTS = 

all: libdram_reallocate.so

%.o : %.cpp
	$(CC) $(CFLAGS) -c $< -o $@
	
libdram_reallocate.so: dram_reallocate.o
	${LINK} ${LFLAGS} -shared -O3 -fPIC -o $@ $^
