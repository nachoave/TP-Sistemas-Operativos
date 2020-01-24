#include "consolaKernel.h"
#include "libKernel.h"

int main(void) {

	//Creacion del archivo de logs
	operacionesTotales = 0;
	flagSalida = 1;
	signal(SIGINT, cerrarPrograma);
	configure_logger();

	//Configuracion del kernel con el archivo de config
	read_and_log_config("configKernel/kernel.config");
	llamadaFunciones = dictionary_create();
	generadorIdScript = 1;
	generadorNumeroLQL = 1;

	dictionary_put(llamadaFunciones, "informacionSobreTablas", &pasaManoDescribe);
	dictionary_put(llamadaFunciones, "devolverListaGossip", &respuestaSobreGossip);
	dictionary_put(llamadaFunciones, "registroUltimoObtenido", &respuestaSelectObtenida);
	dictionary_put(llamadaFunciones, "respuestaDescribeEnUnoDeLFS", &respuestaSobreDescribeSimple);
	dictionary_put(llamadaFunciones, "respuestaDescribeSimple", &respuestaDescribeDeUnaTabla);
	dictionary_put(llamadaFunciones, "respuestaDeLFS", &ejecucionPorRespuesta);
	dictionary_put(llamadaFunciones, "finalizoJournal",&continuarEjecucion);
	dictionary_put(llamadaFunciones, "memoriaPrincipalFull",&ejecutarJournal);

	//Creacion de colas
	colaNew = queue_create();
	colaReady = queue_create();
	colaExec = queue_create();
	colaExit = queue_create();

	sem_init(&esperaRespuesta, 0, 0);
	sem_init(&entradaProcesosEnNew, 0, 0);
	sem_init(&entradaProcesosEnReady, 0, 0);
	sem_init(&puedeRealizarTrabajo, 0, datosConfigKernel->multiProcesamiento);
	sem_init(&paradaHiloExecute, 0, 0);
	sem_init(&controlEjecucion, 0, 0);
	sem_init(&respuestaJournal, 0, 0);
	sem_init(&ejecucionSelect, 0, 0);
	sem_init(&ejecucionInsert, 0, 0);
	sem_init(&ejecucionCreate, 0, 0);
	sem_init(&ejecucionDescribe, 0, 0);
	sem_init(&ejecucionDrop, 0, 0);

	pthread_mutex_init(&iNotify, NULL);
	pthread_mutex_init(&m_colaNew, NULL);
	pthread_mutex_init(&m_colaReady, NULL);
	pthread_mutex_init(&m_colaExit, NULL);
	pthread_mutex_init(&m_Busqueda, NULL);
	pthread_mutex_init(&m_memoriaPrincipal, NULL);
	pthread_mutex_init(&m_agregadoTabla, NULL);
	pthread_mutex_init(&sentenciaSelect, NULL);
	pthread_mutex_init(&m_quantum, NULL);
	pthread_mutex_init(&m_memoria, NULL);
	pthread_mutex_init(&m_operacionesTotales, NULL);

	//Listas Iniciadas
	listaTablasMeta = list_create();
	listaExecutes = list_create();
	listaGossiping = list_create();
	crearListaCriterioDeTablas();

	conectarseConLaPrimerMemoria();

	//Creacion Hilos
    pthread_t hiloConsola, hiloPLP, hiloPCP,hiloGossip, hiloDescribe, hiloMetricas, hiloINotify;
    crearHilosExecute();
    pthread_create(&hiloINotify, NULL, (void*) notificarEventos, "configKernel");
	pthread_create(&hiloPLP, NULL, (void*)&planificadorLargoPlazo, NULL);
	pthread_create(&hiloPCP, NULL, (void*)&planificadorCortoPlazo, NULL);
    pthread_create(&hiloGossip, NULL, (void*) &planificarGossip, NULL);
    pthread_create(&hiloDescribe, NULL, (void*) &planificarDescribeGlobal, NULL);
    //pthread_create(&hiloMetricas, NULL, (void*) &contadorMetricasAutomatico);
	pthread_create(&hiloConsola, NULL, (void*)&consolaKernel, NULL);

  	//Queda Esperando
 	pthread_mutex_init(&mx_main, NULL);
	pthread_mutex_lock(&mx_main);
	pthread_mutex_lock(&mx_main);

	//Finalizacion De Los Hilos
	pthread_join(hiloConsola, NULL);
	pthread_join(hiloPLP, NULL);
	pthread_join(hiloPCP, NULL);
	pthread_join(hiloGossip, NULL);
	pthread_join(hiloDescribe, NULL);
	//Prueba Finalizador De Hilos Execute
	esperaJoinDeLosHilosDeMultiprocesamiento();
	free(datosConfigKernel->ipMemoria);
	free(datosConfigKernel);
	close_logger();
	return EXIT_SUCCESS;
}

void cerrarPrograma(){

	log_info(logger, "Voy A Cerrar Kernel");

	sem_destroy(&entradaProcesosEnNew);
	sem_destroy(&entradaProcesosEnReady);
	sem_destroy(&puedeRealizarTrabajo);
	sem_destroy(&paradaHiloExecute);
	sem_destroy(&controlEjecucion);
	sem_destroy(&ejecucionSelect);
	sem_destroy(&ejecucionInsert);
	sem_destroy(&ejecucionCreate);
	sem_destroy(&ejecucionDescribe);
	sem_destroy(&ejecucionDrop);
	//sem_destroy(&iNotify);

	pthread_mutex_destroy(&m_colaNew);
	pthread_mutex_destroy(&m_colaReady);
	pthread_mutex_destroy(&m_colaExit);
	pthread_mutex_destroy(&m_Busqueda);
	pthread_mutex_destroy(&m_agregadoTabla);

    //queue_destroy_and_destroy_elements(colaNew, (void*)free);
    //queue_destroy_and_destroy_elements(colaReady, (void*)free);
    //queue_destroy_and_destroy_elements(colaExec, (void*)free);
    queue_destroy_and_destroy_elements(colaExit, (void*) liberarProcesosDeExit);

    //list_destroy_and_destroy_elements(listaExecutes, (void*)free);
    list_destroy_and_destroy_elements(listaTablasMeta, (void*)free);
    list_destroy(listaGossiping);
    //list_destroy_and_destroy_elements(tablasCriterios, (void*)free);
    //list_destroy_and_destroy_elements(listaGossiping, (void*)free);
	dictionary_destroy(llamadaFunciones);

    pthread_mutex_unlock(&mx_main);
    pthread_mutex_destroy(&mx_main);
}
