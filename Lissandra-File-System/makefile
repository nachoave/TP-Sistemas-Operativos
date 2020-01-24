sources = src/Lissandra.c  src/consolaLissandra.c src/libLissandra.c
socketLibraries =  ../sample-comunicacion/src/socket.c
pathLibraries = -lreadline -lcommons -lpthread -lm

Lissandra: $(sources)
	gcc -g -o Lissandra $(sources) $(socketLibraries) $(pathLibraries)

clean:
		rm Lissandra
		rm *.o