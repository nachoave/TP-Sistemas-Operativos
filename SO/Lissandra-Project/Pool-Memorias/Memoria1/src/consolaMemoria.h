#ifndef LIB_CONSOLA_H
#define LIB_CONSOLA_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <readline/readline.h>
#include <commons/string.h>
#include "libMemoria.h"

//ENUMERADOR
typedef enum {SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL,SALIR} palabra;

//VARIABLES GLOBALES
char** listaPalabrasReservadas;
palabra palabraReservada;

//FUNCIONES
palabra transformarStringToEnum(char*);
void planificarGossip();
void agregarDireccionPropiaAGossiping();
void agregarMemoriaAListaDeGossiping(nodoGossiping_t*, socket_connection*);
void recibirMensajeParaGossip(socket_connection*, char**);
void envioGossip(socket_connection*, char**);
void procesarInfoGossip(socket_connection*, char**);
bool esTiempoDeEjecutarse(unsigned, unsigned, int);
void consolaMemoria();

#endif
