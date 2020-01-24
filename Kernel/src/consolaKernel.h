#ifndef LIB_CONSOLA_H
#define LIB_CONSOLA_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <readline/readline.h>
#include <commons/string.h>
#include "libKernel.h"

//VARIABLES GLOBALES
palabra palabraReservada;

//FUNCIONES
palabra transformarStringToEnum(char*);
void consolaKernel();

char* retornarSentenciaCompleta(char*, char*);

//Funciones Para La Sentencia Describe
void agregarTabla(tables_Metadata*);

void agregarMemoriaAListaDeGossiping(tabla_Gossiping*);
void agregarSiNoSeEncuentra(char*, criterio_Tablas*);
void agregarMemoriaACriterio(char*, char*);
void respuestaSobreGossip(socket_connection*, char**);

bool verificarSiEsSCYSoloHayUnaMemoria(criterio_Tablas*);
void esperaJoinDeLosHilosDeMultiprocesamiento();
void finalizarHilosActivos();
void liberarElementosDeTabla(tables_Metadata*);

//Funciones Para Armar Un Mini PCB Para El PLP
void crearArchivoLQL(char*);
void crearEstructuraPCB(char*);
void lecturaDeArchivo(char*);
void planificarGossip();
void planificarDescribeGlobal();
bool esTiempoDeEjecutarse(unsigned, unsigned, int);

void respuestaDescribeDeUnaTabla(socket_connection*, char**);
void respuestaSobreDescribeSimple(socket_connection*, char**);
void respuestaSelectObtenida(socket_connection*, char**);
void pasaManoDescribe(socket_connection*, char**);
void ejecucionPorRespuesta(socket_connection*, char**);
void separarTablasEnUnDescribeGlobal(char*);
void continuarEjecucion(socket_connection*);
void ejecutarJournal(socket_connection*);

#endif
