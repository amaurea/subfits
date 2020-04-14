CFLAGS = -g -Wfatal-errors -I$(HOME)/local/include/wcslib

all: subfits_server subfits
subfits_server: subfits_server.o slice_fits.o
	gcc -o $@ $^ -lwcs -lm -pthread
subfits: subfits.o slice_fits.o
	gcc -o $@ $^ -lwcs -lm
%.o: %.c
	gcc -c $(CFLAGS) -o $@ $<
clean:
	rm -rf subfits subfits_server *.o
