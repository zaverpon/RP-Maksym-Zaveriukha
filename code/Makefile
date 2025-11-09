CC = gcc
CFLAGS = -c -O2 -ansi -pedantic -Wall -DLINUX $(INCL)
LFLAGS =
INCL = -I.
LIBS =
RM = rm -rf
AR = ar
ZIP = zip -9 -r

default: libcom.a

libcom.a: com.o
	$(RM) $@
	$(AR) r $@ com.o

com.o: com.c com.h
	$(CC) $(CFLAGS) com.c

main: main.o libcom.a
	$(CC) main.o -L. -lcom -pthread -o main
	@echo "Run with ./main"

main.o: main.c com.h
	$(CC) $(CFLAGS) main.c

zip: clean
	$(ZIP) com`date +'%m%d'`.zip *

clean:
	$(RM) libcom.a com.o main.o main