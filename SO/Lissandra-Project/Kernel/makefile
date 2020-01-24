sources = src/Kernel.c  src/libKernel.c src/consolaKernel.c
socketLibraries = ../sample-comunicacion/src/socket.c
pathLibraries = -lreadline -lcommons -lpthread -lm

Kernel: $(sources)
	gcc -g -o Kernel $(sources) $(socketLibraries) $(pathLibraries)

clean:
		rm Kernel
		rm *.o