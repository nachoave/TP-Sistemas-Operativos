#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libMemoria.h"
#include "consolaMemoria.h"

int main(void){
	//pthread_t hiloINotify;
	socketKernel = -1;
	flagSalida = 1;
	configure_logger();
	signal(SIGINT, cerrarPrograma);
	signal(SIGPIPE, SIG_IGN);
	read_and_log_config("memoria.config");
    log_info(logger, "Voy A Escuchar Por El Puerto: %d", datosConfigMemoria->puertoMemoria);
	llamadaFunciones = dictionary_create();
	sem_init(&iNotify, 0, 1);
	sem_init(&semInserts, 0, 0);
	sem_init(&esperaRespuestaGossip, 0, 0);
	sem_init(&esperaGossip, 0, 0);
	sem_init(&handshakeValue, 0, 0);
	sem_init(&esperarJournal, 0, 0);
	pthread_mutex_init(&m_agregarMemoria, NULL);
	pthread_mutex_init(&m_bitarray, NULL);
	pthread_mutex_init(&m_journal, NULL);
	pthread_mutex_init(&m_segmento, NULL);
	pthread_mutex_init(&m_pagina, NULL);

	//Funciones De Diccionario
	dictionary_put(llamadaFunciones, "identificarProcesoConMemoria", &identificarConKernel);
	dictionary_put(llamadaFunciones, "realizarGossipConMemoriaPrincipal", &identificarConKernel);
	dictionary_put(llamadaFunciones, "conocerAlKernel", &agregarAlKernel);
	dictionary_put(llamadaFunciones, "comunicarTamanioValue", &obtenerTamanioPalabra);
	dictionary_put(llamadaFunciones, "identificarProcesoConDescribe", &identificarProcesoALFS);
	dictionary_put(llamadaFunciones, "resultadoAlIdentificarseConKernel", &pasarDescribeAlIdentificarse);
	dictionary_put(llamadaFunciones, "enviarUnSelectALFS", &pasaManoSelect);
	dictionary_put(llamadaFunciones, "enviarUnCreateALFS", &pasaManoCreate);
	dictionary_put(llamadaFunciones, "enviarUnInsertALFS", &insertarOActualizarKey);
	dictionary_put(llamadaFunciones, "enviarUnDropALFS", &pasaManoDrop);
	dictionary_put(llamadaFunciones, "enviarDescribeALFS", &pasarDescribeALFS);
	dictionary_put(llamadaFunciones, "valorEncontradoEnSelect", &enviarUltimoRegistro);
	dictionary_put(llamadaFunciones, "pasarInfoDescribe", &devolverDescribeSimple);
	dictionary_put(llamadaFunciones, "pasarDescribeGlobalEnUno", &pasarDescribeGlobalEnUno);
	dictionary_put(llamadaFunciones, "pasarInfoGossip", &recibirMensajeParaGossip);
	dictionary_put(llamadaFunciones, "pasarTambienInfoGossip", &envioGossip);
	dictionary_put(llamadaFunciones, "recibirTablaGossip", &procesarInfoGossip);
	dictionary_put(llamadaFunciones, "realizarSecuenciaJournal", &journalAPedidoDeKernel);
	dictionary_put(llamadaFunciones, "forzarJournal", &forzarJournalParaInsert);
	//Probablemente Lo Reduzca A Una Misma Palabra De Diccionario
	dictionary_put(llamadaFunciones, "respuestaSobreSelect", &devolverRespuesta);
	dictionary_put(llamadaFunciones, "respuestaSobreCreate", &devolverRespuesta);
	dictionary_put(llamadaFunciones, "respuestaSobreInsert", &devolverRespuesta);
	dictionary_put(llamadaFunciones, "respuestaSobreDrop", &devolverRespuesta);
	dictionary_put(llamadaFunciones, "respuestaSobreDescribe", &devolverRespuesta);
	//Hasta Aca

	//pthread_create(&hiloINotify, NULL, (void*) notificarEventos, "/home/utnso/workspace/memoria1/Pool-Memorias");

	inicializarMemoria();

    agregarDireccionPropiaAGossiping();

    pthread_t hiloGossip, hiloConsola, hiloJournal;

    pthread_create(&hiloGossip, NULL, (void*) &planificarGossip, NULL);
	pthread_create(&hiloConsola, NULL, (void*) &consolaMemoria, NULL);
	pthread_create(&hiloJournal, NULL, (void*) &controladorJournal, NULL);

  	//Queda Esperando
 	pthread_mutex_init(&mx_main, NULL);
	pthread_mutex_lock(&mx_main);
	pthread_mutex_lock(&mx_main);
	pthread_join(hiloConsola,NULL);
	pthread_join(hiloGossip,NULL);
	pthread_join(hiloJournal, NULL);
	return EXIT_SUCCESS;
}

void cerrarPrograma(){

	log_info(logger, "Voy A Cerrar Memoria");

	close_logger();
	liberarDatosMemoria(datosConfigMemoria);
	free(bitarrayMarcos->bitarray);
	bitarray_destroy(bitarrayMarcos);
	dictionary_destroy(llamadaFunciones);
	list_clean_and_destroy_elements(listaDeGossiping, (void*) eliminarMemoriasConocidas);
	liberarMemoria();
	pthread_mutex_destroy(&m_bitarray);
	pthread_mutex_destroy(&m_agregarMemoria);
	pthread_mutex_destroy(&m_journal);
    pthread_mutex_unlock(&mx_main);
    pthread_mutex_destroy(&mx_main);
}

