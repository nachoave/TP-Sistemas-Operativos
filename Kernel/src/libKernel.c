#include <stdio.h>
#include <stdlib.h>
#include "libKernel.h"


//Creacion Del Archivo Para Logs
void configure_logger() {

	char * nombrePrograma = "kernel.log";
	char * nombreArchivo = "Kernel";

	logger = log_create(nombrePrograma, nombreArchivo, false, LOG_LEVEL_TRACE);
	log_info(logger, "Se genero log del Kernel");
}

t_config_Kernel * read_and_log_config(char* path) {

	char* ipMem;

	log_info(logger, "Voy a leer el archivo kernel.config");

	t_config* archivo_Config = config_create(path);
	if (archivo_Config == NULL) {
		log_error(logger, "No existe archivo de configuracion");
		exit(1);
	}

	datosConfigKernel = malloc(sizeof(t_config_Kernel));

	ipMem = string_new();
	string_append(&ipMem,  config_get_string_value(archivo_Config,"IP_MEMORIA"));
	datosConfigKernel->ipMemoria = ipMem;

	datosConfigKernel->puertoMemoria = config_get_int_value(archivo_Config,"PUERTO_MEMORIA");;
	datosConfigKernel->quantum = config_get_int_value(archivo_Config, "QUANTUM");
	datosConfigKernel->multiProcesamiento = config_get_int_value(archivo_Config, "MULTIPROCESAMIENTO");
	datosConfigKernel->metadataRefresh = config_get_int_value(archivo_Config, "METADATA_REFRESH");
	datosConfigKernel->sleepEjecucion = (config_get_int_value(archivo_Config, "SLEEP_EJECUCION")/1000);
	datosConfigKernel->tiempoGossiping = config_get_int_value(archivo_Config, "RETARDO_GOSSIPING");

	log_info(logger, "IP_MEMORIA: %s", datosConfigKernel->ipMemoria);
	log_info(logger, "PUERTO_MEMORIA: %d", datosConfigKernel->puertoMemoria);
	log_info(logger, "QUANTUM: %d", datosConfigKernel->quantum);
	log_info(logger, "MULTIPROCESAMIENTO: %d", datosConfigKernel->multiProcesamiento);
	log_info(logger, "METADATA_REFRESH: %d", datosConfigKernel->metadataRefresh);
	log_info(logger, "SLEEP_EJECUCION: %d", datosConfigKernel->sleepEjecucion);
	log_info(logger, "RETARDO_GOSSIPING: %d", datosConfigKernel->tiempoGossiping);

	log_info(logger, "Fin de lectura");

	config_destroy(archivo_Config);
    return datosConfigKernel;

}

void notificarEventos(char* path) {

	char buffer[BUF_LEN];

	int file_descriptor = inotify_init();
	if (file_descriptor < 0) {
		log_error(logger, "An Error Occur With INotify_Init");
		return;
	}

	int watch_descriptor = inotify_add_watch(file_descriptor, path, IN_MODIFY);

	while (flagSalida) {

		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0) {
			log_error(logger, "Error Trying To Use Read");
			inotify_rm_watch(file_descriptor, watch_descriptor);
			close(file_descriptor);
			return;
		}

		int offset = 0;

		while (offset < length) {

			struct inotify_event *event =
					(struct inotify_event *) &buffer[offset];

			if (event->len) {

				if (event->mask & IN_MODIFY) {
					if (event->mask & IN_ISDIR) {
						log_info(logger, "The Directory %s was modified.\n",
								event->name);
					} else {
						pthread_mutex_lock(&iNotify);
						char* copiaPath = string_new();
						string_append(&copiaPath, path);
						string_append(&copiaPath, "/kernel.config");
						log_info(logger, "The file %s was modified.\n",
								event->name);
						t_config* archivo_Config = config_create(copiaPath);
						datosConfigKernel->quantum = config_get_int_value(archivo_Config, "QUANTUM");
						datosConfigKernel->metadataRefresh = config_get_int_value(
								archivo_Config, "METADATA_REFRESH");
						datosConfigKernel->sleepEjecucion = (config_get_int_value(archivo_Config, "SLEEP_EJECUCION")/1000);
						config_destroy(archivo_Config);
						free(copiaPath);
						pthread_mutex_unlock(&iNotify);
					}

				}
			}

			offset += sizeof(struct inotify_event) + event->len;
		}

		inotify_rm_watch(file_descriptor, watch_descriptor);
		watch_descriptor = inotify_add_watch(file_descriptor, path, IN_MODIFY);
	}

	inotify_rm_watch(file_descriptor, watch_descriptor);
	close(file_descriptor);
	return;
}

char* retornarSentenciaCompleta(char* primeraPalabra, char* segundaPalabra){

if (segundaPalabra == NULL) {
	return primeraPalabra;
}

else {
	string_append_with_format(&primeraPalabra, " %s",segundaPalabra);
	return primeraPalabra;
}
}

tables_Metadata* retornarElementoTabla(tables_Metadata* tablaElemento){

	int index = 0;
	tables_Metadata* elementoTabla = list_get(listaTablasMeta, index);

	while(elementoTabla != NULL){

		if(string_contains(elementoTabla->nombreTabla, tablaElemento->nombreTabla)){
			return elementoTabla;
		}

		index++;
		elementoTabla = list_get(listaTablasMeta, index);
	}

	return NULL;
}

tables_Metadata* retornarTabla(tables_Metadata* tablaElemento){

	int index = 0;
	tables_Metadata* elementoTabla = list_get(listaTablasMeta, index);

	while(elementoTabla != NULL){

		if(elementoTabla->nombreTabla == tablaElemento->nombreTabla){
			return elementoTabla;
		}

		index++;
		elementoTabla = list_get(listaTablasMeta, index);
	}

	return NULL;
}

void crearListaCriterioDeTablas() {

	t_list* criterios = list_create();
	list_add(criterios, "SC");
	list_add(criterios, "EC");
	list_add(criterios, "SHC");

	tablasCriterios = list_create();

	for(int i = 0; i < list_size(criterios) ;i++){
		criterio_Tablas* criterioCreado = malloc(sizeof(criterio_Tablas));
		criterioCreado->tipoConsistencia = list_get(criterios, i);
		criterioCreado->tiempoInserts = 0;
		criterioCreado->tiempoSelects = 0;
		criterioCreado->memoriasAsignadas = list_create();
		list_add(tablasCriterios, criterioCreado);
	}

	log_info(logger, "Se Crearon Las Tablas De Consistencia");
	list_destroy(criterios);
}

void planificadorCortoPlazo(){

	pcb_t* pcbAEjecutar;

	while(flagSalida){

		if(queue_size(colaReady) == 0){
			log_trace(logger, "Se Espera A Que Haya PCB's En La Cola Ready");
		}

		sem_wait(&entradaProcesosEnReady);
		sem_wait(&puedeRealizarTrabajo);

		if(!flagSalida){ break; }

		pthread_mutex_lock(&m_Planificador);
		pcbAEjecutar = desencolarPCB(colaReady,m_colaReady);

		if(pcbAEjecutar->quantum == 0){
			pthread_mutex_lock(&iNotify);
			pcbAEjecutar->quantum = 3;
			pthread_mutex_unlock(&iNotify);
		}
		pthread_mutex_unlock(&m_Planificador);

		pthread_mutex_lock(&m_Busqueda);
		execute_t* executeDisponible = list_find(listaExecutes, (void*) noSeEncuentraEjecutando);
		executeDisponible->proceso = pcbAEjecutar;
		sem_post(&executeDisponible->enEspera);
		pthread_mutex_unlock(&m_Busqueda);
	}
	log_info(logger, "Cerrando El PCP De Hilo %d", pthread_self());
	return;
}

void planificadorLargoPlazo(){

	while(flagSalida){
		sem_wait(&entradaProcesosEnNew);
		if(!flagSalida){ break; }
		tiny_pcb* miniPCB = desencolarRutaScript(colaNew, m_colaNew);
		char* duplicadoRuta = string_duplicate(miniPCB->ruta);
		pcb_t* scriptPCB = malloc(sizeof(pcb_t));
		scriptPCB->idProceso = generadorIdScript;
		generadorIdScript++;
		scriptPCB->pathScript = duplicadoRuta;
		scriptPCB->programCounter = 0;
		scriptPCB->estado = READY;
		pthread_mutex_lock(&iNotify);
		scriptPCB->quantum = datosConfigKernel->quantum;
		pthread_mutex_unlock(&iNotify);
		scriptPCB->flagFinalizacion = 0;
		encolarPCB(colaReady, scriptPCB, m_colaReady);
		free(miniPCB->ruta);
		free(miniPCB);
		sem_post(&entradaProcesosEnReady);
	}
	log_info(logger, "Cerrando El PLP De Hilo %d", pthread_self());
	return;
}

bool noSeEncuentraEjecutando(execute_t* execLibre){
	return execLibre->proceso == NULL;
}

void crearHilosExecute(){

	pthread_t hiloExec;
	pcb_t* unPCBVacio = NULL;

	for(int i = 0; i < datosConfigKernel->multiProcesamiento; i++){
		pthread_create(&hiloExec, NULL, (void*) estadoEjecucionActivo, NULL);
		execute_t* hiloEjecucion = malloc(sizeof(execute_t));
		hiloEjecucion->idHiloExecute = hiloExec;
		hiloEjecucion->proceso = unPCBVacio;
		sem_init(&hiloEjecucion->enEspera ,0 ,0);
		list_add(listaExecutes, hiloEjecucion);
		sem_post(&paradaHiloExecute);
	}
}

void estadoEjecucionActivo() {
	sem_wait(&paradaHiloExecute);
	//Buscar Su Propio Id De HiloExecute Para Poder Bloquearse Y A La Espera De Su Post En EL PCP Para Ejecutar
	bool coincideElIdExecuteCorrespondiente(execute_t* hiloExecute) {
		return hiloExecute->idHiloExecute == pthread_self();
	}

	pthread_mutex_lock(&m_Busqueda);
	execute_t* hiloPropio = list_find(listaExecutes,
			(void*) coincideElIdExecuteCorrespondiente);
	pthread_mutex_unlock(&m_Busqueda);

	while (flagSalida) {
		log_trace(logger, "Hilo %d - Esperando", hiloPropio->idHiloExecute);
		sem_wait(&hiloPropio->enEspera);
		if(!flagSalida){ break; }
		log_info(logger, "Se Va A Ejecutar El Script - %s", hiloPropio->proceso->pathScript);
		hiloPropio->proceso = ejecutarSentenciasEnLineas(hiloPropio->proceso);
		//sleep(datosConfigKernel->sleepEjecucion);

		if (hiloPropio->proceso->flagFinalizacion) {
			log_info(logger,"Se Pasa El Script - %s A La Cola De Finalizacion",hiloPropio->proceso->pathScript);
			printf("Se Pasa El Script - %s A La Cola De Finalizacion\n",hiloPropio->proceso->pathScript);
			encolarPCB(colaExit, hiloPropio->proceso, m_colaExit);
			hiloPropio->proceso = NULL;
			log_trace(logger, "Procesos Finalizados - %d", queue_size(colaExit));
		}

		else {
			log_info(logger, "Se Pasa El Proceso Con Id = %d A La Cola De Ready",hiloPropio->proceso->idProceso);
			encolarPCB(colaReady, hiloPropio->proceso, m_colaReady);
			hiloPropio->proceso = NULL;
			sem_post(&entradaProcesosEnReady);
		}

		sem_post(&puedeRealizarTrabajo);
	}
	printf("Cerrando El Hilo Exec = %d\n", hiloPropio->idHiloExecute);
	return;
}

void encolarPCB(t_queue* cola, pcb_t* proceso, pthread_mutex_t mutex){

	pthread_mutex_lock(&mutex);
	queue_push(cola, proceso);
	pthread_mutex_unlock(&mutex);
}

pcb_t* desencolarPCB(t_queue* cola, pthread_mutex_t mutex){

	pthread_mutex_lock(&mutex);
	pcb_t* proceso = queue_pop(cola);
	pthread_mutex_unlock(&mutex);
	return proceso;
}

void encolarRutaScript(t_queue* cola, tiny_pcb* miniPCB, pthread_mutex_t mutex){

	pthread_mutex_lock(&mutex);
	queue_push(cola, miniPCB);
	pthread_mutex_unlock(&mutex);
}

tiny_pcb* desencolarRutaScript(t_queue* cola, pthread_mutex_t mutex){

	pthread_mutex_lock(&mutex);
	tiny_pcb* miniPCB = queue_pop(cola);
	pthread_mutex_unlock(&mutex);
	return miniPCB;
}

pcb_t* ejecutarSentenciasEnLineas(pcb_t* procesoPasado){

	pthread_t hiloControlEjecucion;
	pcb_t* procesoAEjecutar = procesoPasado;
	opSentencia_t sentencia;
	char* tipoConsistencia;
	tiempo_Memorias* identificadorMemoria;
	tabla_Gossiping* memoriaElegida;
	int posicionRandom;
	int cantidadMemoriasConocidas;

	while(procesoAEjecutar->quantum != 0){

		sentencia = obtenerSentenciaParseada(procesoAEjecutar->pathScript, procesoAEjecutar->programCounter);

		switch (sentencia.palabraReservada) {

		case SELECT:

			if (!(verificarSiExisteTabla(sentencia.parte1))) {
			log_error(logger, "La %s Se Desconoce", sentencia.parte1);
			procesoAEjecutar->flagFinalizacion = 1;
			return procesoAEjecutar;
			}

			tipoConsistencia = retornarConsistenciaDeTabla(sentencia.parte1);
			identificadorMemoria = retornarIdentificadorMemoria(tipoConsistencia, sentencia.parte2);

			if(identificadorMemoria == NULL){
				log_error(logger, "No Hay Memoria Para Asignar El Trabajo En %s", tipoConsistencia);
				log_trace(logger, "Se Devuelve El Proceso %d A La Cola De Trabajo", procesoAEjecutar->idProceso);
				return procesoAEjecutar;
			}

			memoriaElegida = devolverMemoriaAEnviar(identificadorMemoria->numeroMemoria);
			log_trace(logger, "Se Asigno El Trabajo A La Memoria %s", memoriaElegida->numeroMemoria);
			runFunction(memoriaElegida->socketMemoria, "enviarUnSelectALFS", 2, sentencia.parte1, sentencia.parte2);
			unsigned tiempoInicialEnvio = time(NULL);
			sem_wait(&ejecucionSelect);
			sleep(datosConfigKernel->sleepEjecucion);
			unsigned tiempoRespuestaEnvio = time(NULL);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);
			unsigned tiempoResultado = tiempoRespuestaEnvio - tiempoInicialEnvio;
			pthread_mutex_lock(&m_memoria);
			sumarTiempoALaConsistencia(tiempoResultado, tipoConsistencia, sentencia.palabraReservada);
			identificadorMemoria->contadorSelects++;
			pthread_mutex_unlock(&m_memoria);

			if(respuestaSelect == -1){
				log_error(logger, "No Se Pudo Realizar El Select");
				procesoAEjecutar->flagFinalizacion = 1;
				//pthread_mutex_unlock(&sentenciaSelect);
				return procesoAEjecutar;
			}
			//pthread_mutex_unlock(&sentenciaSelect);
			break;

		case INSERT:

			if (!(verificarSiExisteTabla(sentencia.parte1))) {
				log_error(logger, "La %s Se Desconoce", sentencia.parte1);
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}

			tipoConsistencia = retornarConsistenciaDeTabla(sentencia.parte1);
			identificadorMemoria = retornarIdentificadorMemoria(tipoConsistencia, sentencia.parte2);

			if(identificadorMemoria == NULL){
				log_error(logger, "No Hay Memoria Para Asignar El Trabajo En %s", tipoConsistencia);
				log_trace(logger, "Se Devuelve El Proceso %d A La Cola De Trabajo", procesoAEjecutar->idProceso);
				return procesoAEjecutar;
			}

			memoriaElegida = devolverMemoriaAEnviar(identificadorMemoria->numeroMemoria);
			log_trace(logger, "Se Asigno El Trabajo A La Memoria %s", memoriaElegida->numeroMemoria);

			runFunction(memoriaElegida->socketMemoria, "enviarUnInsertALFS", 3, sentencia.parte1, sentencia.parte2, sentencia.parte3);
			unsigned tiempoInicialInsert = time(NULL);
			sem_wait(&ejecucionInsert);
			sleep(datosConfigKernel->sleepEjecucion);
			unsigned tiempoRespuestaInsert = time(NULL);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);
			unsigned tiempoResultadoInsert = tiempoRespuestaInsert - tiempoInicialInsert;
			pthread_mutex_lock(&m_memoria);
			sumarTiempoALaConsistencia(tiempoResultadoInsert, tipoConsistencia, sentencia.palabraReservada);
			identificadorMemoria->contadorInserts++;
			pthread_mutex_unlock(&m_memoria);

			if(!respuestaInsert){
				log_error(logger, "No Se Pudo Realizar El Insert");
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}
			break;

		case CREATE:

			identificadorMemoria = retornarIdentificadorMemoria(sentencia.parte2, NULL);

			if(identificadorMemoria == NULL){
				log_error(logger, "No Hay Memoria Para Asignar El Trabajo En %s", sentencia.parte2);
				log_trace(logger, "Se Devuelve El Proceso %d A La Cola De Trabajo", procesoAEjecutar->idProceso);
				return procesoAEjecutar;
			}

			memoriaElegida = devolverMemoriaAEnviar(identificadorMemoria->numeroMemoria);

			log_trace(logger, "Se Asigno El Trabajo A La Memoria %s Para %s", memoriaElegida->numeroMemoria, sentencia.parte2);

			runFunction(memoriaElegida->socketMemoria, "enviarUnCreateALFS", 4, sentencia.parte1, sentencia.parte2, sentencia.parte3, sentencia.parte4);
			sem_wait(&ejecucionCreate);
			sleep(datosConfigKernel->sleepEjecucion);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);

			if(!respuestaCreate){
				log_error(logger, "No Se Pudo Realizar El Create");
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}
			break;

		case DESCRIBE:

			posicionRandom = rand() % list_size(listaGossiping);
			memoriaElegida = list_get(listaGossiping, posicionRandom);

			if(sentencia.parte1 != NULL){

				runFunction(memoriaElegida->socketMemoria, "enviarDescribeALFS", 1, sentencia.parte1);
			}
			else{
				runFunction(memoriaElegida->socketMemoria, "enviarDescribeALFS", 1, "GLOBAL");
			}
			sem_wait(&ejecucionDescribe);
			sleep(datosConfigKernel->sleepEjecucion);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);

			if(!respuestaDescribe){
				log_error(logger, "No Se Pudo Realizar El Describe");
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}
			break;

		case DROP:

			if (!(verificarSiExisteTabla(sentencia.parte1))) {
				log_error(logger, "La %s Se Desconoce", sentencia.parte1);
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}

			tipoConsistencia = retornarConsistenciaDeTabla(sentencia.parte1);
			identificadorMemoria = retornarIdentificadorMemoria(tipoConsistencia, NULL);

			if(identificadorMemoria == NULL){
				log_error(logger, "No Hay Memoria Para Asignar El Trabajo En %s", tipoConsistencia);
				log_trace(logger, "Se Devuelve El Proceso %d A La Cola De Trabajo", procesoAEjecutar->idProceso);
				return procesoAEjecutar;
			}

			memoriaElegida = devolverMemoriaAEnviar(identificadorMemoria->numeroMemoria);

			log_trace(logger, "Se Asigno El Trabajo A La Memoria %s", memoriaElegida->numeroMemoria);

			eliminarTablaDeLosMetadata(sentencia.parte1);

			runFunction(memoriaElegida->socketMemoria, "enviarUnDropALFS", 1, sentencia.parte1);
			sem_wait(&ejecucionDrop);
			sleep(datosConfigKernel->sleepEjecucion);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);

			if(!respuestaDrop){
				log_error(logger, "No Se Pudo Realizar El Drop");
				procesoAEjecutar->flagFinalizacion = 1;
				return procesoAEjecutar;
			}
			break;

		case JOURNAL:

			cantidadMemoriasConocidas = list_size(listaGossiping);

			for (int i = 0; i < cantidadMemoriasConocidas; i++) {

				tabla_Gossiping* elementoMemoria = list_get(listaGossiping, i);

				bool estaAsociadaAUnCriterio(criterio_Tablas* elemento) {

					bool tienenElMismoIdentificador(tiempo_Memorias* memoria) {
						return string_contains(memoria->numeroMemoria,
								elementoMemoria->numeroMemoria);
					}

					tiempo_Memorias* memoryNumber = list_find(elemento->memoriasAsignadas,
							(void*) tienenElMismoIdentificador);
					return memoryNumber != NULL;
				}

				if (list_any_satisfy(tablasCriterios,
						(void*) estaAsociadaAUnCriterio)){
					log_info(logger, "Ejecutando Journal Con Memoria %s", elementoMemoria->numeroMemoria);
					runFunction(elementoMemoria->socketMemoria,
							"realizarSecuenciaJournal", 0);

				sem_wait(&respuestaJournal);
				}
			}
			sleep(datosConfigKernel->sleepEjecucion);
			pthread_mutex_lock(&m_operacionesTotales);
			operacionesTotales++;
			pthread_mutex_unlock(&m_operacionesTotales);
			break;
		}

	procesoAEjecutar->quantum--;
	procesoAEjecutar->programCounter++;

	if(sentencia.ultimaSentencia){
		destruirOperacionSentencia(sentencia);
		log_info(logger, "El Proceso Con Id %d Finalizo", procesoAEjecutar->idProceso);
		procesoAEjecutar->flagFinalizacion = 1;
		return procesoAEjecutar;
	}

	destruirOperacionSentencia(sentencia);
	}
	return procesoAEjecutar;
}

tabla_Gossiping* devolverMemoriaAEnviar(char* identificadorMemoria){

	bool coincideElIdentificador(tabla_Gossiping* self){

		return string_contains(self->numeroMemoria, identificadorMemoria);
	}

	return list_find(listaGossiping, (void*) coincideElIdentificador);
}

opSentencia_t obtenerSentenciaParseada(char* script,int programCounter){
	opSentencia_t sentenciaParseada;
    char* linea = NULL;
    size_t len = 0;
    ssize_t read;
	int i;

	FILE* archivo = abrirScript(script);
	for(i=0; i<programCounter; i++){
		getline(&linea, &len, archivo);
	}

	if( read = getline(&linea, &len, archivo) != -1){
		log_trace(logger, "Sentencia: %s", linea);
		sentenciaParseada = parser(linea);
		int prevPos = ftell(archivo);

		read = getline(&linea, &len, archivo);
		if( read == -1){
			sentenciaParseada.ultimaSentencia = true;
		}
			fseek(archivo, prevPos, SEEK_SET);
	}
	else{
		sentenciaParseada.palabraReservada == 10;
		log_error(logger, "El LQL ya no tiene mas sentencias ");
	}

    if (linea){
        free(linea);
	}
	fclose(archivo);
	return sentenciaParseada;
}

opSentencia_t parser(char* lineaAParsear){

	char comilla = '"';
	char espaciosEnBlanco[4] = " \n\t";

	//Estructura A Retornar
	opSentencia_t retorno;
	char* lineaAuxiliar = string_duplicate(lineaAParsear);
	string_trim(&lineaAuxiliar);
	char** splitPalRes = string_n_split(lineaAuxiliar, 2, " \n\t");
	char** splitResto = NULL;
	char** splitRestoInsert = NULL;
	char* palReservada = splitPalRes[0];

	if(string_contains(palReservada, "DESCRIBE") || string_contains(palReservada, "JOURNAL")){}

	else if(string_contains(palReservada, "INSERT")){

		splitResto = string_n_split(splitPalRes[1], 3, " \n\t");
		splitRestoInsert = string_n_split(splitResto[2], 2, &comilla);
	}

	else{
		splitResto = string_n_split(splitPalRes[1], 4, " \n\t");
	}

//SELECT, INSERT, CREATE, DESCRIBE, DROP
	if(string_equals_ignore_case(palReservada, "SELECT")){
		retorno.palabraReservada = SELECT;
		retorno.parte1 = splitResto[0];
        retorno.parte2 = splitResto[1];
        retorno.parte3 = NULL;
        retorno.parte4 = NULL;
	} else if(string_equals_ignore_case(palReservada, "INSERT")){
		retorno.palabraReservada = INSERT;
		retorno.parte1 = splitResto[0];
		retorno.parte2 = splitResto[1];
		retorno.parte3 = splitRestoInsert[0];
		retorno.parte4 = NULL;
	} else if(string_equals_ignore_case(palReservada, "CREATE")){
		retorno.palabraReservada = CREATE;
		retorno.parte1 = splitResto[0];
		retorno.parte2 = splitResto[1];
		retorno.parte3 = splitResto[2];
		retorno.parte4 = splitResto[3];
	} else if(string_equals_ignore_case(palReservada, "DESCRIBE")){
		retorno.palabraReservada = DESCRIBE;
		if(splitPalRes[1] != NULL){
			retorno.parte1 = splitPalRes[1];
		}
		else{retorno.parte1 = NULL;}
		retorno.parte2 = NULL;
		retorno.parte3 = NULL;
		retorno.parte4 = NULL;
	} else if(string_equals_ignore_case(palReservada, "DROP")){
		retorno.palabraReservada = DROP;
		retorno.parte1 = splitResto[0];
		retorno.parte2 = NULL;
        retorno.parte3 = NULL;
        retorno.parte4 = NULL;
	} else if(string_equals_ignore_case(palReservada, "JOURNAL")){
		retorno.palabraReservada = JOURNAL;
		retorno.parte1 = NULL;
		retorno.parte2 = NULL;
        retorno.parte3 = NULL;
        retorno.parte4 = NULL;
	}

	retorno.ultimaSentencia = false;
	free(splitPalRes[0]);
	free(splitPalRes);
	free(splitResto);
	if(splitRestoInsert != NULL){
		free(splitRestoInsert[1]);
	}
	free(splitRestoInsert);
	free(lineaAuxiliar);
	return retorno;
}

////Libero La Estructura Una Vez Utilizada En La Comunicacion Con Memoria
void destruirOperacionSentencia(opSentencia_t operacion){
	if(operacion.parte1 != NULL)
		free(operacion.parte1);
	if(operacion.parte2 != NULL)
		free(operacion.parte2);
	if(operacion.parte3 != NULL)
		free(operacion.parte3);
	if(operacion.parte4 != NULL)
		free(operacion.parte4);
}

FILE * abrirScript(char * scriptFilename){

  char* direccionLocal = "LISSANDRA_FS/Archivos/Scripts/";
  char* ruta = string_new();
  string_append(&ruta, direccionLocal);
  string_append(&ruta, scriptFilename);

  FILE * scriptf = fopen(ruta, "r");

  if (scriptf == NULL)
  {
    log_error(logger, "Error al abrir el archivo %s", scriptFilename);
    exit(EXIT_FAILURE);
  }

  free(ruta);
  return scriptf;
}

void funcionEspera(char* tipoEjecucion){

	if(string_contains(tipoEjecucion, "SELECT")){
		sem_wait(&ejecucionSelect);
	}
	else if(string_contains(tipoEjecucion, "INSERT")){
		sem_wait(&ejecucionInsert);
	}
	else if(string_contains(tipoEjecucion, "CREATE")){
		sem_wait(&ejecucionCreate);
	}
	else if(string_contains(tipoEjecucion, "DESCRIBE")){
		sem_wait(&ejecucionDescribe);
	}
	else if(string_contains(tipoEjecucion, "DROP")){
		sem_wait(&ejecucionDrop);
	}

	log_trace(logger, "Liberando Hilo Para Continuar");
}

//Funcion Solamente Para Las Respuestas De CREATE-INSERT-DROP
//args[0]: Sentencia, args[1]: Respuesta Por "0" o "1
void ejecucionPorRespuesta(socket_connection* connection, char** args){

	if(string_contains(args[0], "SELECT")){
		pthread_mutex_lock(&sentenciaSelect);
		respuestaSelect = atoi(args[1]);
		sem_post(&ejecucionSelect);
	}

	else if(string_contains(args[0], "CREATE")){
		respuestaCreate = atoi(args[1]);
		sem_post(&ejecucionCreate);
	}
	else if(string_contains(args[0], "INSERT")) {
		respuestaInsert = atoi(args[1]);
		sem_post(&ejecucionInsert);
	}
	else if(string_contains(args[0], "DESCRIBE")){
		respuestaDescribe = atoi(args[1]);
		sem_post(&ejecucionDescribe);
	}

	else if(string_contains(args[0], "DROP")) {
		respuestaDrop = atoi(args[1]);
		sem_post(&ejecucionDrop);
	}

	else if(string_contains(args[0], "JOURNAL")) {
		sem_post(&respuestaJournal);
	}
}

void respuestaSelectObtenida(socket_connection* connection, char** args) {

	//pthread_mutex_lock(&sentenciaSelect);
	respuestaSelect = atoi(args[0]);

	if (respuestaSelect == 1) {
		char* registroReciente = string_duplicate(args[1]);
		log_info(logger, "El Registro Reciente Obtenido Fue: %s",
				registroReciente);
		free(registroReciente);
	} else {

		char* key = string_duplicate(args[2]);
		char* tabla = string_duplicate(args[1]);

		log_error(logger, "No Se Encontro La Key: %s En La %s", key, tabla);
		free(key);
		free(tabla);
	}
	sem_post(&ejecucionSelect);
}

char* retornarConsistenciaDeTabla(char* nombreTabla){

	bool tienenElMismoIdentificador(tables_Metadata* self){
		return string_contains(self->nombreTabla, nombreTabla);
	}

	tables_Metadata* tablaEncontrada = list_find(listaTablasMeta, (void*) tienenElMismoIdentificador);
	return tablaEncontrada->tipoConsistencia;
}

tiempo_Memorias* retornarIdentificadorMemoria(char* tipoConsistencia, char* key){

	bool devolverListaDeTipoConsistencia(criterio_Tablas* self){
		return string_contains(self->tipoConsistencia, tipoConsistencia);
	}

	criterio_Tablas* listaConsistencia = list_find(tablasCriterios, (void*) devolverListaDeTipoConsistencia);

	if(list_is_empty(listaConsistencia->memoriasAsignadas)){
		return NULL;
	}

	else if(string_contains(listaConsistencia->tipoConsistencia, "SC")){
		return list_get(listaConsistencia->memoriasAsignadas, 0);
	}

	else if(string_contains(listaConsistencia->tipoConsistencia, "EC")){
	int posicionAleatoria = rand() % list_size(listaConsistencia->memoriasAsignadas); // Empieza Desde El 0 Hasta El Tamanio Max De La Lista
	return list_get(listaConsistencia->memoriasAsignadas, posicionAleatoria);
	}

	else if(string_contains(listaConsistencia->tipoConsistencia, "SHC") && key == NULL){

	int posicionAle = rand() % list_size(listaConsistencia->memoriasAsignadas); // Empieza Desde El 0 Hasta El Tamanio Max De La Lista
	return list_get(listaConsistencia->memoriasAsignadas, posicionAle);
	}

	int numeroKey = atoi(key);
	int cantidadMemorias = list_size(listaConsistencia->memoriasAsignadas);
	int moduloResto = numeroKey % cantidadMemorias;

	return list_get(listaConsistencia->memoriasAsignadas, moduloResto);
}

void conectarseConLaPrimerMemoria(){

	pthread_mutex_lock(&m_memoriaPrincipal);
	socketMemoria = connectServer(datosConfigKernel->ipMemoria, datosConfigKernel->puertoMemoria, llamadaFunciones, &disconnect, NULL);
	pthread_mutex_unlock(&m_memoriaPrincipal);
	if(socketMemoria == -1){
		log_error(logger, "No Se Pudo Conectar A La Memoria Principal");
		return;
	}
	log_info(logger, "Me Conecte Con La Memoria Principal");
	runFunction(socketMemoria, "identificarProcesoConMemoria" ,1, "Kernel");
	sem_wait(&esperaRespuesta);
	int tamanioLista = list_size(listaGossiping);
	int posicionRandom = rand() % tamanioLista;
	tabla_Gossiping* memoriaAleatoria = list_get(listaGossiping, posicionRandom);
	if(runFunction(memoriaAleatoria->socketMemoria, "identificarProcesoConDescribe" ,1, "Kernel")){
		sem_wait(&ejecucionDescribe);
	}
}

void ejecucionAutomaticaDescribe(){

	unsigned inicioTimeDescribe = (unsigned) time(NULL);
	int tiempoDescribeMecanico = datosConfigKernel->metadataRefresh;
	int valorRetardo = (datosConfigKernel->sleepEjecucion / 1000);
	int contadorDeDescribe = 1;

	while(flagSalida){

		sleep(valorRetardo);
		int tamanioLista = list_size(listaGossiping);
		int posicionRandom = rand() % tamanioLista;
		tabla_Gossiping* memoriaAleatoria = list_get(listaGossiping, posicionRandom);
		if(runFunction(memoriaAleatoria->socketMemoria, "identificarProcesoConDescribe", 1, "Kernel")){
		sem_wait(&ejecucionDescribe);
		}
	}

	log_trace(logger,"Se Sale Del Programa De Describe");
}

bool esTiempoDeRealizarUnDescribe(unsigned inicioDeDescribe, unsigned nuevoTiempo, int tiempoDescribeMecanico){
	return (nuevoTiempo - inicioDeDescribe) == tiempoDescribeMecanico;
}

void sumarTiempoALaConsistencia(unsigned tiempoResultado, char* consistenciaBuscada, palabra palabraReservada){

	bool tienenLaMismaConsistencia(criterio_Tablas* nodoCriterio){
		return string_contains(nodoCriterio->tipoConsistencia, consistenciaBuscada);
	}

	criterio_Tablas* criterioEncontrado = list_find(tablasCriterios, (void*) tienenLaMismaConsistencia);

	if(palabraReservada == SELECT){
	criterioEncontrado->tiempoSelects += tiempoResultado;
	}
	else if (palabraReservada == INSERT){
	criterioEncontrado->tiempoInserts += tiempoResultado;
	}
}

//Cierre Del Archivo De Logs
void close_logger() {
	log_info(logger, "Cierro log del Kernel");
	log_destroy(logger);
}

void disconnect(socket_connection* connection) {
	log_info(logger, "El socket n°%d se ha desconectado.", connection->socket);

	bool memoriaQueSeDesconecto(tabla_Gossiping* memoriaABuscar){
		return connection->socket == memoriaABuscar->socketMemoria;
	}

	pthread_mutex_lock(&m_memoriaPrincipal);
	tabla_Gossiping* memoriaDesconectada = list_remove_by_condition(listaGossiping, (void*) memoriaQueSeDesconecto);

	if(memoriaDesconectada->puertoMemoria == datosConfigKernel->puertoMemoria){
		socketMemoria = -1;
	}

	removerMemoriaDeLaTablaDeConsistencia(memoriaDesconectada->numeroMemoria);
	log_info(logger, "El Memory Number: %s Del Socket: %d Se Elimino De Las Estructuras", memoriaDesconectada->numeroMemoria, connection->socket);
	free(memoriaDesconectada->numeroMemoria);
	free(memoriaDesconectada->ipMemoria);
	free(memoriaDesconectada);
	pthread_mutex_unlock(&m_memoriaPrincipal);
}

void removerMemoriaDeLaTablaDeConsistencia(char* numeroMemoria){

	int cantidadCriterios = list_size(tablasCriterios);

	for(int i = 0; i < cantidadCriterios; i++){

		criterio_Tablas* elementoCriterio = list_get(tablasCriterios, i);

		bool seEncuentraSuMemoryNumber(tiempo_Memorias* memoriaNumero){
			return string_contains(memoriaNumero->numeroMemoria, numeroMemoria);
		}

		pthread_mutex_lock(&m_memoria);
		tiempo_Memorias* numeroMemoriaRemovido = list_remove_by_condition(elementoCriterio->memoriasAsignadas, (void*) seEncuentraSuMemoryNumber);
		pthread_mutex_unlock(&m_memoria);

		if(numeroMemoriaRemovido != NULL){
		free(numeroMemoriaRemovido->numeroMemoria);
		free(numeroMemoriaRemovido);
		}
	}
}

void continuarEjecucion(socket_connection* connection){
	sem_post(&respuestaJournal);
}

void ejecutarJournal(socket_connection* connection)
{
	log_debug(logger,"memoriaPrincipalFull");
	runFunction(connection->socket,"forzarJournal",0);
}

void liberarProcesosDeExit(pcb_t* proceso){
	free(proceso->pathScript);
	free(proceso);
}
