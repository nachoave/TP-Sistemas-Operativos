#ifndef LIB_Kernel_H
#define LIB_Kernel_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <signal.h>
#include <semaphore.h>
#include <pthread.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/dictionary.h>
#include <commons/collections/queue.h>
#include <commons/collections/list.h>
#include <math.h>
#include  "../../sample-comunicacion/src/socket.h"

#define EVENT_SIZE ( sizeof (struct inotify_event) + 15)
#define BUF_LEN ( 1024 * EVENT_SIZE )

typedef struct {
	char* ipMemoria;
	int puertoMemoria;
	int quantum;
	int multiProcesamiento;
	int metadataRefresh;
	int sleepEjecucion;
	int tiempoGossiping;
} t_config_Kernel;

//ENUMERADOR
typedef enum {SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL, ADD, RUN, METRICS, SALIR} palabra;
typedef enum {NEW, READY, EXECUTE, EXIT} status_pcb;

//Estructura Del PCB
typedef struct {
	int idProceso;
	char* pathScript;
	int programCounter;
	status_pcb estado;
	int quantum;
	bool flagFinalizacion;
}pcb_t;

typedef struct {
	char* ruta;
	status_pcb estado;
} tiny_pcb;

typedef struct{
	int idHiloExecute;
	pcb_t* proceso;
	sem_t enEspera;
} execute_t;

typedef struct {
	char* nombreTabla;
	char* tipoConsistencia;
	int cantidadParticiones;
	int tiempoCompactacion;
}tables_Metadata;

typedef struct {
	char* tipoConsistencia;
	unsigned tiempoInserts;
	unsigned tiempoSelects;
	t_list* memoriasAsignadas;
}criterio_Tablas;

typedef struct {
	char* numeroMemoria;
	int contadorInserts;
	int contadorSelects;
}tiempo_Memorias;

typedef struct {
	char* numeroMemoria;
	int socketMemoria;
	char* ipMemoria;
	int puertoMemoria;
}tabla_Gossiping;

typedef struct{
    palabra palabraReservada;
    char* parte1;
    char* parte2;
    char* parte3;
    char* parte4;
	bool ultimaSentencia;
}opSentencia_t;

t_list* listaExecutes;
t_list* listaTablasMeta;
t_list* tablasCriterios;
t_list* listaGossiping;

t_config_Kernel* datosConfigKernel;
t_log* logger;
t_log* logger_debug;

tabla_Gossiping* memoriasConocidas;

//Variables Globales
int operacionesTotales;
int socketMemoria;
int flagSalida;
int generadorIdScript;
int generadorNumeroLQL;
t_dictionary* llamadaFunciones;
pthread_mutex_t mx_main;

//Respuestas Globales
int respuestaSelect;
bool respuestaInsert;
bool respuestaCreate;
bool respuestaDescribe;
bool respuestaDrop;

//Semaforos Globales
sem_t esperaRespuesta;
sem_t entradaProcesosEnNew;
sem_t entradaProcesosEnReady;
sem_t puedeRealizarTrabajo;
sem_t paradaHiloExecute;
sem_t controlEjecucion;
sem_t respuestaJournal;
sem_t ejecucionSelect;
sem_t ejecucionInsert;
sem_t ejecucionCreate;
sem_t ejecucionDescribe;
sem_t ejecucionDrop;

//Mutex Para Colas
pthread_mutex_t iNotify;
pthread_mutex_t m_colaNew;
pthread_mutex_t m_colaReady;
pthread_mutex_t m_colaExit;
//Creacion de semaforo dependiendo el grado de multiprocesamiento.
pthread_mutex_t mutexQueueExec;
pthread_mutex_t m_Planificador;
pthread_mutex_t m_Busqueda;
pthread_mutex_t m_agregadoTabla;
pthread_mutex_t m_memoriaPrincipal;
pthread_mutex_t sentenciaSelect;
pthread_mutex_t m_quantum;
pthread_mutex_t m_memoria;
pthread_mutex_t m_operacionesTotales;

//Colas Globales
t_queue* colaNew;
t_queue* colaReady;
t_queue* colaExec;
t_queue* colaExit;

//LOGS
void configure_logger();
void close_logger();
t_config_Kernel * read_and_log_config(char*);
void notificarEventos(char*);

void conectarseConLaPrimerMemoria();
tables_Metadata* retornarElementoTabla(tables_Metadata*);
void crearListaCriterioDeTablas();

//Funciones De Planificadores
void planificadorLargoPlazo();
void planificadorCortoPlazo();
void crearHilosExecute();
void estadoEjecucionActivo();

//Funcion De Hilos
bool noSeEncuentraEjecutando(execute_t* execLibre);
bool esTiempoDeRealizarUnDescribe(unsigned, unsigned, int);
void funcionEspera();

//Funciones Para Trabajo De Colas
void encolarPCB(t_queue*, pcb_t*, pthread_mutex_t);
pcb_t* desencolarPCB(t_queue*, pthread_mutex_t);
void encolarRutaScript(t_queue*, tiny_pcb*, pthread_mutex_t);
tiny_pcb* desencolarRutaScript(t_queue*, pthread_mutex_t);
void liberarProcesosDeExit(pcb_t*);

//Funciones Sobre Estructura Admin De Tablas
//Funcion Para Verificar Si Ya Existe La Tabla Para Que No Continue
bool verificarSiExisteTabla(char*);
//Funcion Para La Sentencia Drop
void eliminarTablaDeLosMetadata(char* nombreTabla);
char* retornarConsistenciaDeTabla(char*);
tiempo_Memorias* retornarIdentificadorMemoria(char*, char*);
tabla_Gossiping* devolverMemoriaAEnviar(char*);

//Funciones Para Leer Las Sentencias
pcb_t* ejecutarSentenciasEnLineas(pcb_t*);
opSentencia_t obtenerSentenciaParseada(char*,int);
FILE * abrirScript(char*);
opSentencia_t parser(char*);
void sumarTiempoALaConsistencia(unsigned, char*, palabra);
void logueoDeMetricasManual();
void eliminarMemoriasContador(tiempo_Memorias*);
void destruirOperacionSentencia(opSentencia_t);
void disconnect(socket_connection*);
void removerMemoriaDeLaTablaDeConsistencia(char*);
void cerrarPrograma();

#endif
