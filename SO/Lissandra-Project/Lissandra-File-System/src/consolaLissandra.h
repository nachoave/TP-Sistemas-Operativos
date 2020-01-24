#ifndef LIB_CONSOLA_H
#define LIB_CONSOLA_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <readline/readline.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include  "../../sample-comunicacion/src/socket.h"

//ENUMERADOR
typedef enum {SELECT, INSERT, CREATE, DESCRIBE, DROP, SALIR} palabra;
typedef uint16_t uint16;

////Estructura Para Crear La Tabla y Generar Su Metadata
typedef struct{
	char* nombreTabla;
	char* key;
}datos_Select;

typedef struct{
	char* nombreTabla;
	char* key;
	char* value;
	char* timestamp;
}datos_Insert;

typedef struct{
	char* nombreTabla;
	char* tipoConsistencia;
	char* numeroDeParticiones;
	char* tiempoDeCompactacion;
}datos_Create;

//VARIABLES GLOBALES
int flagEscucha;
palabra palabraReservada;
datos_Select* infoSelect;
datos_Insert* infoInsert;
datos_Create* infoCreacion;

//FUNCIONES
palabra transformarStringToEnum(char*);
int lecturaSegunLaLineaDefinida(char*);
void consolaLissandra();
void controlEjecucionCompactacion(void*);
char* enviarTablasCreadasEnUno(char*, char*, char*);
void enviarTablaEspecificada(socket_connection*, char*, char*);
//Funcion Para Crear La Tabla, Su Metadata En Su Directorio
void crear_y_almacenar_datos(socket_connection*, datos_Create*);
//Funcion Para Recorrer La Estructura De Arbol
char* recorridoDeTodasLasTablasPersistidas();
//Funcion Para Recorrer Una Sola Rama De Arbol
void recorridoDeUnaTabla(socket_connection*, char*);
//Funcion Para Eliminar Todos Los Archivos Dentro Del Directorio Y Por Ultimo El Mismo
void destruirEnCascadaElDirectorio(socket_connection*, char*);
//Funcion Para Recorrer Las Tablas Creadas E Ingresar Un Registro Al Final De La Lista
void insertarValoresDentroDeLaMemTable(socket_connection*, datos_Insert*);
//Funcion Que Pasa Por Todos Los Distintos Archivos De Una Tabla, Compara y Devuelve El Maximo
void seleccionarYEncontrarUltimaKeyActualizada(socket_connection*, datos_Select*);
void planificarDump();
bool esTiempoDeEjecutarse(unsigned, unsigned, int);
void eliminarContenidoAlmacenado(char*, pthread_mutex_t);
bool hayBloquesParaReservar(char*);
int cantidadDeBloquesLibres();
int retornarTiempoDeCompactacionARealizar(char*);

#endif
