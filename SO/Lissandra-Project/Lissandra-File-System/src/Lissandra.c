#include "libLissandra.h"
#include "consolaLissandra.h"

int main(void) {

	flagSalida = 1;
	//Al Realizar ctrl-c Llama A La Funcion CerrarPrograma Para Liberar Recursos
    signal(SIGINT, cerrarPrograma);
	sem_init(&iNotify, 0, 1);
	//sem_init(&dumpSem, 0, 1);
    sem_init(&esperaTabla, 0, 0);

    pthread_mutex_init(&m_bitarray, NULL);
    pthread_mutex_init(&m_metadata, NULL);
    pthread_mutex_init(&m_busqueda, NULL);
    pthread_mutex_init(&m_inserts, NULL);
    pthread_mutex_init(&m_insercion, NULL);

	//  Creacion De Listas
	listaDeMemTable = list_create();
	listadoTablas = list_create();

	configure_logger();
	read_and_log_config("configLissandra/lissandra.config");
	crearPuntoDeMontajeYTablesLFS();
	llamadaFunciones = dictionary_create();

	//Funciones De Diccionario
	dictionary_put(llamadaFunciones, "identificarProcesoMemoria", &identificarProceso);
	dictionary_put(llamadaFunciones, "realizarDescribePorIdentificacionConLFS", &pasarDescribePorId);
	dictionary_put(llamadaFunciones, "realizarDescribeConLFS", &pasarDescribePorId);
	dictionary_put(llamadaFunciones, "realizarDropEnLFS", &realizarDropPorComunicacion);
	dictionary_put(llamadaFunciones, "mensajeDropeoDeMemoria", &mensajeDropDeMemoria);
	dictionary_put(llamadaFunciones, "realizarDefinidoDescribeConLFS", &realizarDescribePorComunicacion);
	dictionary_put(llamadaFunciones, "mensajeDescribeDeMemoria", &mensajeDescribeDeMemoria);
	dictionary_put(llamadaFunciones, "realizarSelectEnLFS", &realizarSelectPorComunicacion);
	dictionary_put(llamadaFunciones, "realizarCreateEnLFS", &realizarCreatePorComunicacion);
	dictionary_put(llamadaFunciones, "mensajeCrearDeMemoria", &mensajeCreateDeMemoria);
	dictionary_put(llamadaFunciones, "realizarInsertEnLFS", &realizarInsertPorComunicacion);

	establecerLaMetadataDeLFS();
	crearArchivosBinTotalesFS();
	cantidadTablas = verificarTablasExistentes();
	crearBitMap();
	recorrerTablasExistentesParaOcuparBloques();

	//pthread_create(&hiloINotify, NULL, (void*) notificarEventos, "configLissandra");
	pthread_t hiloDump;
	pthread_create(&hiloDump, NULL, (void*) planificarDump, NULL);

//  Iniciando La Estructura Del Servidor
	log_info(logger, "Voy A Escuchar Por El Puerto: %d", datosConfigLissandra->puertoEscucha);
    createListen(datosConfigLissandra->puertoEscucha, NULL, llamadaFunciones, &disconnect, NULL);

    //  Uso De Consola
	pthread_t hiloConsola;
	pthread_create(&hiloConsola, NULL, (void*) &consolaLissandra, NULL);

  	//Queda Esperando
 	pthread_mutex_init(&mx_main, NULL);
	pthread_mutex_lock(&mx_main);
	pthread_mutex_lock(&mx_main);

	pthread_join(hiloConsola, NULL);
	pthread_join(hiloDump, NULL);

	return EXIT_SUCCESS;
}

void cerrarPrograma(){

	log_info(logger, "Voy A Cerrar LFS");
	//  Destruccion De Carpeta Metadata Y Bloques
	liberarMapeoEnMemoria();
	bitarray_destroy(bitarray);
	destruirBloquesLFS();
	destruirCarpetaMetadata();
	destruirMontajeYTables();

	//pthread_join(hiloINotify, NULL);
	//pthread_join(hiloDump, NULL);
	sem_destroy(&iNotify);
	close_logger();
	dictionary_destroy(llamadaFunciones);
	//Liberacion De Datos
	liberarReferenciasDeTablas(cantidadTablas);
	liberarDatosDelMetadata(datosConfigLissandra);
	liberarDatosFileSystem(datosFileSystem);

    pthread_mutex_unlock(&mx_main);
    pthread_mutex_destroy(&mx_main);
}
