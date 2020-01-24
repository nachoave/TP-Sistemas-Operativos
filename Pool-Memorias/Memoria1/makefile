sources = src/Memoria.c  src/consolaMemoria.c src/libMemoria.c
socketLibraries = ../../sample-comunicacion/src/socket.c
pathLibraries = -lreadline -lcommons -lpthread -lm

Memoria: $(sources)
	gcc -g -o Memoria $(sources) $(socketLibraries) $(pathLibraries)

clean:
		rm Memoria
		rm *.o