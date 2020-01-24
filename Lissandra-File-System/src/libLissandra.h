#ifndef LIB_Lissandra_H
#define LIB_Lissandra_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <pthread.h>
#include <semaphore.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <time.h>
#include <math.h>
#include <commons/log.h>
#include <commons/txt.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/dictionary.h>
#include <commons/collections/queue.h>
#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include "consolaLissandra.h"

#define EVENT_SIZE ( sizeof (struct inotify_event) + 15)
#define BUF_LEN ( 1024 * EVENT_SIZE )

//Definicion De Todas Las Estructuras A Utilizar
typedef struct {
	int puertoEscucha;
	char* ptoMontaje;
	int retardo;
	int tamanioValue;
	int tiempoDump;
}t_config_Lissandra;

typedef struct {
	size_t tamanioBloques;
	int cantidadBloques;
	char* magicNumber;
}t_data_FS;

typedef struct {
	char* nombreTabla;
	t_list* list_Table;
}t_memTable_LFS;

typedef struct {
	char* nombreTabla;
	int contadorDumpeos;
	int cantidadCompactados;
	pthread_mutex_t mutex_tabla;
	pthread_t hiloTabla;
	int flagSalida;
}t_createdTables;

typedef struct {
	char* timeStamp;
	char* key;
	char* value;
}data_list_Table;

typedef struct {
	int numeroParticion;
	t_list* nodosParticion;
}list_Particiones;

//Variables Globales
//Definidas Propias
t_log* logger;
t_config_Lissandra* datosConfigLissandra;
t_data_FS* datosFileSystem;

//Sobre Semaforos
sem_t iNotify;
sem_t dumpSem;
sem_t esperaTabla;

//Sobre Hilos
//Normales
pthread_t hiloINotify;

//Mutex
pthread_mutex_t mx_main;
pthread_mutex_t m_bitarray;
pthread_mutex_t m_metadata;
pthread_mutex_t m_inserts;
pthread_mutex_t m_busqueda;
pthread_mutex_t m_insercion;

//Para La Mutua Exclusion
//Sobre Listas
t_list* listadoTablas;
t_list* listaDeMemTable;
//Sobre Variables
t_bitarray* bitarray;

//Otros Sin Condicion De Carrera
t_dictionary* llamadaFunciones;
int cantidadTablas;
int flagSalida; //Manejado Solo Por El Hilo Consola
struct dirent** resultados;

//LOGS
void configure_logger();
t_config_Lissandra * read_and_log_config(char*);
void close_logger();

//Funciones De Creacion De Archivos Y Directorios
void crearPuntoDeMontajeYTablesLFS();
void establecerLaMetadataDeLFS();
void crearBitMap();
void crearParticiones(char*, char*);
void crearArchivosBinTotalesFS();
void setearMetadataBloques(char*);

//Funciones De Destruccion De Archivos Y Directorios
void destruirMontajeYTables();
void destruirCarpetaMetadata();
void destruirBloquesLFS();

//Funciones De Retorno De Valores
char* retornarDireccionMetadata();
char* retornarDireccionBloques();
char* retornarDireccionPrincipalConTabla(char*);
t_createdTables*retornarElementoTabla(char*);
t_memTable_LFS* retornarElementoMemTable(char*);
char retornarUltimoCaracter(char*);
void retornarCantidadParticionTabla(char*, int*);
int  retornarCantidadDeBlocksEnParticion(char**);

//Funciones De Verificacion
int  verificarTablasExistentes();
int  verificarTablaEnFS(char*);
bool valueMenorQueElMaximo(char*);
bool tamanioKeyMenorQueElMaximo(char*);

//Funciones Utilizando BitArray
int establecerBloqueYDatosAOcupar(char*);
void liberarBloquesDeParticion(char*);
int  buscarBloqueLibre();
void recrearBloqueVacio(char*);
void reservarArrayBloques(int[] , int);
void recorrerTablasExistentesParaOcuparBloques();
void obtenerElementosPersistidos(t_createdTables* nombreTabla);

//Funciones Sobre Registros
void insertarRegistroDentroDelBloque(char*, char*);
void iniciarProcesoDUMP();
void iniciarEnUnaTablaLaCOMPACTACION(char*);
char* recortarExtractoPrimero(char*, int);
char* recortarExtractoSegundo(char*, int);
void acotarTodosLosArchivosCompactadosDeLaTabla(char*, t_createdTables*);
void descargarRegistrosALista(t_list*, int*, char*, t_createdTables*);
void actualizacionDeRegistros(t_list*, char*, t_createdTables*);
void meterRegistrosEnLaLista(list_Particiones*, FILE*);
void modificarParticionesOriginales(t_list*, int*, char*, t_createdTables*);
void insertarValoresEnTabla(datos_Insert* , t_memTable_LFS* );
char* buscarLineaMaxima(char*, char*, unsigned* ,char*, t_createdTables*);

//Otras Funciones
void   mostrarInfoTabla(char*, char*);
int    calculoModuloParticion(int, int);
void   notificarEventos(char*);
double devolverTamanioTotalDeArchivo(list_Particiones*);

//Funciones De Comunicacion
void identificarProceso(socket_connection*,char**);
void pasarDescribePorId(socket_connection*, char**);
void realizarSelectPorComunicacion(socket_connection*, char**);
void realizarInsertPorComunicacion(socket_connection*, char**);
void realizarCreatePorComunicacion(socket_connection*, char**);
void mensajeCreateDeMemoria(socket_connection*, char**);
void realizarDescribePorComunicacion(socket_connection*, char**);
void mensajeDescribeDeMemoria(socket_connection*, char**);
void realizarDropPorComunicacion(socket_connection*, char**);
void mensajeDropDeMemoria(socket_connection*, char**);
void disconnect(socket_connection*);

//Funciones De Liberacion De Datos
void liberarReferenciasDeTablas(int);
void liberarMapeoEnMemoria();
void liberarDatos(t_memTable_LFS*);
void table_destroy(t_memTable_LFS*);
void liberarSiOcupaDatos(char*);
void liberarDatosDelMetadata(t_config_Lissandra*);
void liberarDatosFileSystem(t_data_FS*);
void duplicados_destroy(void*);
void liberarElementos(list_Particiones*);
void liberarRegistrosEnMemTable(data_list_Table*);
void eliminarDatosDeMemTable(t_memTable_LFS*);
void eliminarRegistrosEnMem(data_list_Table*);

//Funcion De Cierre De Todo El Programa
void cerrarPrograma();
#endif
