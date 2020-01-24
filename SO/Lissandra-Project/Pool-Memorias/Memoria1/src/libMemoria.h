#ifndef LIB_Memoria_H
#define LIB_Memoria_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <semaphore.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/dictionary.h>
#include <commons/collections/queue.h>
#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include  "../../../sample-comunicacion/src/socket.h"
#include <math.h>

#define EVENT_SIZE ( sizeof (struct inotify_event) + 15)
#define BUF_LEN ( 1024 * EVENT_SIZE )
#define	FN_SUCCESS	1	/* Successful exit status. if(FN_SUCCESS) entra  */
#define	FN_ERROR	0	/* Error exit status. if(FN_ERROR) no entra */
#define	NO_ENCONTRO_MARCO_LIBRE	-1	/* No encontro marco libre (en el bit array o en la memoria aplicando LRU) */
#define	MAX_TIME_TAMP	9000000000	/* maximo timestamp imposible */

typedef unsigned tipoNroPagina;

typedef struct {
	int puertoMemoria;
	char* ipFileSystem;
	int puertoFS;
	char** ipVectorMemorias;
	char** puertosSeeds;
	int retardoMemoria;
	int retardoFS;
	tipoNroPagina tamanioMemoria;
	int retardoJournal;
	int retardoGossiping;
	char* memoryNumber;
} t_config_Memoria;

typedef struct
{
	char* nombreDeLaTabla;
	t_list* tablaDePaginas;
} nodoSegmento_t;

typedef struct
{
	tipoNroPagina nroPagina;
	bool flags;
	int frame; // apunta a un registro de tamanio variable (ts key value)
	double timeStamp; // sirve para el algoritmo LRU // TODO inicializar en 0
} nodoPagina_t;

typedef struct
{
	char* timestamp;
	char* key;
	char* value;
} marco_t;

typedef struct
{
	char* memoryNumber;
	int socket;
	char* ip;
	int puerto;
} nodoGossiping_t;

typedef struct{
	int socketConexion;
	int puerto;
} conexionGossip_t;

typedef struct{
	char* tabla;
	char* key;
	char* valor;
	double timestamp;
	socket_connection* connection;
} INSERT_MEMORIA_t;

typedef struct{
	char* tabla;
	char* key;
} SELECT_MEMORIA_t;

int vectorSocket[10];
int flagSalida;
int socketLissandra;
int socketKernel;
t_config_Memoria* datosConfigMemoria;
t_log* logger;
conexionGossip_t* puertoSocket;

//Variables Globales Para Semaforos
sem_t iNotify;
sem_t handshakeValue;
sem_t semInserts;
sem_t esperaGossip;
sem_t esperaRespuestaGossip;
sem_t esperarJournal;

//Listas Globales
t_list* tablaDeSegmentos;
t_list* listaDeGossiping;

//Array Globales
t_bitarray* bitarrayMarcos;

//Otras Variables Globales
int socketLFS;
t_dictionary* llamadaFunciones;
pthread_mutex_t mx_main;
pthread_mutex_t m_agregarMemoria;
pthread_mutex_t m_bitarray;
pthread_mutex_t m_journal;
pthread_mutex_t m_segmento;
pthread_mutex_t m_pagina;
char* pMemoriaPrincipal; //donde inicia el gran malloc
int flagSalida;
int tamanioValue;
size_t tamanioDeMarco;
int cantidadMarcos;
INSERT_MEMORIA_t* insert_mem_struct;
SELECT_MEMORIA_t* select_mem_struct;
nodoPagina_t* elementoCreado;

//LOGS
void configure_logger();
void close_logger();
t_config_Memoria * read_and_log_config(char*);
char* mostrarConfiguracionConexion(char**);
int retornarCantidadDeBlocksEnParticion(char**);

//funciones
//int INSERT_MEMORIA(); // devuelve FN_SUCCESS si ya existe la tabla en la lista de segmentos. Caso contrario devuelve FN_ERROR.
int JOURNAL_MEMORIA(socket_connection*); // journal. Devuelve FN_SUCCESS/FN_ERROR.
void eliminarSegmentosYSusPaginas(nodoSegmento_t*);
/**
* @NAME: INSERT_MEMORIA
* @DESC: Inserta en memoria el registro.
*/
nodoPagina_t* INSERT_MEMORIA(INSERT_MEMORIA_t*); // insert principal. retorna nodo de la pagina
void SELECT_MEMORIA(SELECT_MEMORIA_t*, socket_connection*); // Realiza el select principal.
void DROP_MEMORIA(char* tabla, socket_connection*); //dropea la tabla
void SELECT_LFS(char* tabla, char* key); //Busca la tabla y la key en el LFS. Si existe almacena la tabla y la key, sino retorna loguea error.
void controladorJournal();
void* controladorGossiping();
void inicializarMemoria();
void* controladorNuevaConexion(void *pCliente);
void* notificarEventos(char*);
nodoSegmento_t* crearTablaSegmento(char* nombreDeLaTabla); // inserto la tabla.
nodoPagina_t* crearNodoPagina(t_list* tablaDePaginas, bool flagModificado, int posicionFrameLibre, uint16_t key, char* value, tipoNroPagina posicionMarcoLibreBitarray); // inserto la pagina en lista de paginas.
nodoPagina_t* insertaOActualizaPagina(nodoSegmento_t* segmentoTabla, char* nombreTabla, char* keyString, char* value, double timestamp, socket_connection* connection);  // inserta o acutaliza. retorna la pagina
int yaExisteLaTabla(char* nombreDeLaTabla); // devuelve FN_SUCCESS si ya existe la tabla en la lista de segmentos. Caso contrario devuelve FN_ERROR.
nodoSegmento_t* buscarSegmentoPorNombreDeTabla(char *nombreDeLaTabla); // devuelve el nodo del segmento encontrado, caso contrario NULL
char* copiarStr(const char * palabra); // reserva memoria para copiar un string.
int validarTamanioValue(char* value);
bool tamanioKeyMayorQueElMaximo(int keyTamanio);
void liberarMemoria();
tipoNroPagina setearMarcoLibreEnBitarray(); // devuelve la posicion del bitarray de un marco libre y lo setea que en ocupado (1). Si no encuentra devuelve -1
tipoNroPagina buscarMarcoLibreEnBitarray(uint16_t keyUint16, char* value); // busca marco libre en bitarray y devuelve FN_SUCCESS, caso contrario si no encuentra un marco libre devuelve FN_ERROR.
tipoNroPagina insertarEnMemoriaPrincipal(uint16_t keyUint16, char* value, double timestamp); // inserta en mem princ y devuelve FN_SUCCESS, caso contrario si no encuentra un marco libre devuelve FN_ERROR.
tipoNroPagina algoritmoLRU(nodoSegmento_t* segmentoTabla, char* keyString, char* value, double timestamp); // Algoritmo LRU de paginas. Elimina la pagina menos usada y devuelve el nro de pagina (alias marco) a reemplazar.
tipoNroPagina obtenerPaginaEnMemoria(char* tabla, t_list* listaPaginas, char* key, socket_connection* connection); // buscar la key en memoria y devuelve el nro de pagina (bitarray)
tipoNroPagina buscarMarcoLibreEnMemoria(); //setea un marco libre en ocupado y devuelve la posicion.
void bitarrayImprimir1eros100();
void imprimirBitarray();
char* enviarTablaGossipEnUno();
void SEL_INS_MEM(INSERT_MEMORIA_t* insert_struct); //Otra Funcion Fuera De enviarUltimoRegistro

//Funciones De Comunicacion
void identificarConKernel(socket_connection*, char**);
void agregarAlKernel(socket_connection*, char**);
void pasaManoSelect(socket_connection*, char**);
void enviarUltimoRegistro(socket_connection*, char**);
void pasaManoCreate(socket_connection*, char**);
void insertarOActualizarKey(socket_connection*, char**);
void pasarDescribeAlIdentificarse(socket_connection*, char**);
void devolverDescribeSimple(socket_connection*, char**);
void pasarDescribeGlobalEnUno(socket_connection*, char**);
void pasarDescribeALFS(socket_connection*, char**);
void pasaManoDrop(socket_connection*, char**);
void devolverRespuesta(socket_connection*, char**);
void identificarProcesoALFS(socket_connection* ,char**);
void obtenerTamanioPalabra(socket_connection*,char**);
void disconnect(socket_connection*);
double getCurrentTime(); // obtiene timestamp en milisegundos (issue 1313) usage: double firstReport = getCurrentTime(); printf("%f\n", firstReport);
void guardarPaginaEnMP(tipoNroPagina nroMarco, uint16_t keyUint16, char* value, double timestamp); //Guarda en MP los valores de la pagina
void imprimirMP();
marco_t* obtenerRegistroDesdeMemoria(nodoPagina_t* nodoDeUnaPagina);
void limpiarMarcos();
void liberarDatosMemoria(t_config_Memoria* datosConfigMemoria);
void cerrarPrograma();
void realizarGossip();
int retornarCantidadDeElementos(char** listaBloques);
void journalAPedidoDeKernel(socket_connection*);
void forzarJournalParaInsert(socket_connection*);
void eliminarMemoriasConocidas(nodoGossiping_t*);


#endif
