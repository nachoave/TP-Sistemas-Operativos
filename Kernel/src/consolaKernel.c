#include "consolaKernel.h"

void consolaKernel(){

	char* linea = NULL;
	char espaciosEnBlanco[4]=" \n\t";
	int continuar = 1;

	char* primeraPalabra = NULL;
	char* segundaPalabra = NULL;
	char* terceraPalabra = NULL;
	char* cuartaPalabra = NULL;
	char* quintaPalabra = NULL;

	do{
		free(linea);
		log_info(logger, "Bienvenido A La Consola, Ingrese Una Sentencia");
		linea = readline(">");

		char* lineaDuplicada = string_duplicate(linea);
		string_to_upper(lineaDuplicada);
		primeraPalabra = strtok(linea, espaciosEnBlanco);
		segundaPalabra = strtok(NULL, espaciosEnBlanco);
		terceraPalabra = strtok(NULL, espaciosEnBlanco);
		cuartaPalabra = strtok(NULL, espaciosEnBlanco);
		quintaPalabra = strtok(NULL, espaciosEnBlanco);

		string_to_upper(primeraPalabra);
		palabraReservada = transformarStringToEnum(primeraPalabra);

		switch(palabraReservada){

		case SELECT:
				string_to_upper(segundaPalabra);
				crearArchivoLQL(lineaDuplicada);
				break;
		case INSERT:
				string_to_upper(segundaPalabra);
				crearArchivoLQL(lineaDuplicada);
				break;
		case CREATE:
				string_to_upper(segundaPalabra);
				crearArchivoLQL(lineaDuplicada);
				break;

		case JOURNAL:

				crearArchivoLQL("JOURNAL");
				break;

		case DESCRIBE:

				if (segundaPalabra != NULL){
				string_to_upper(segundaPalabra);
				}

				crearArchivoLQL(lineaDuplicada);
				break;

		case DROP:
				string_to_upper(segundaPalabra);
				crearArchivoLQL(lineaDuplicada);
				break;
		case ADD:
				string_to_upper(quintaPalabra);
				agregarMemoriaACriterio(terceraPalabra, quintaPalabra);
				break;

		case RUN:
				crearEstructuraPCB(segundaPalabra);
				break;

		case METRICS:
				logueoDeMetricasManual();
				break;

		case SALIR:
				continuar = strcmp(linea, "SALIR");
				if(continuar) printf("comando no reconocido\n");
				break;
		default:
				break;
		}
		free(lineaDuplicada);
	}while(continuar);
		flagSalida = 0;
		finalizarHilosActivos();
		free(linea);
		return;
	}

palabra transformarStringToEnum(char* palReservada) {

	palabra palabraTransformada;

			if (string_contains(palReservada, "SELECT")) {
				palabraTransformada = SELECT;
			}

			else if (string_contains(palReservada, "INSERT")) {
				palabraTransformada = INSERT;
			}

			else if (string_contains(palReservada, "CREATE")) {
				palabraTransformada = CREATE;
			}

			else if (string_contains(palReservada, "DESCRIBE")) {
				palabraTransformada = DESCRIBE;
			}

			else if (string_contains(palReservada, "DROP")) {
				palabraTransformada = DROP;
			}

			else if (string_contains(palReservada, "JOURNAL")) {
				palabraTransformada = JOURNAL;
			}

			else if (string_contains(palReservada, "ADD")) {
				palabraTransformada = ADD;
			}

			else if (string_contains(palReservada, "RUN")) {
				palabraTransformada = RUN;
			}

			else if (string_contains(palReservada, "METRICS")){
				palabraTransformada = METRICS;
			}

			else if (string_contains(palReservada, "SALIR")) {
				palabraTransformada = SALIR;
			}

			return palabraTransformada;
		}

void agregarTabla(tables_Metadata* tabla) {
	pthread_mutex_lock(&m_agregadoTabla);
	if(list_is_empty(listaTablasMeta)){
		list_add(listaTablasMeta, tabla);
		log_info(logger, "Se Agrego %s En La Lista", tabla->nombreTabla);
		pthread_mutex_unlock(&m_agregadoTabla);
		return;
	}

	int indice = 0;
	tables_Metadata* tablaElemento = list_get(listaTablasMeta, indice);

	while (tablaElemento != NULL) {

		if (string_contains(tablaElemento->nombreTabla,tabla->nombreTabla)) {
			log_info(logger, "La %s Ya Existe En La Lista", tabla->nombreTabla);
			free(tabla);
			pthread_mutex_unlock(&m_agregadoTabla);
			return;
		}

		indice++;
		tablaElemento = list_get(listaTablasMeta, indice);
	}

	list_add(listaTablasMeta, tabla);
	pthread_mutex_unlock(&m_agregadoTabla);
	log_info(logger, "Se Agrego %s En La Lista", tabla->nombreTabla);
}

bool verificarSiExisteTabla(char* segundaPalabra){

	bool existeNombreTabla(tables_Metadata* tablaMeta){
	return string_contains(tablaMeta->nombreTabla, segundaPalabra);
	}

	return list_any_satisfy(listaTablasMeta, (void*) existeNombreTabla);
}

void eliminarTablaDeLosMetadata(char* nombreTabla){

bool existeNombreTabla(tables_Metadata* tablaMeta) {

	return string_contains(tablaMeta->nombreTabla, nombreTabla);
}

list_remove_and_destroy_by_condition(listaTablasMeta,
		(void*) existeNombreTabla, (void*) liberarElementosDeTabla);
log_info(logger, "Se Removio De La Estructura La %s",
		nombreTabla);

}

void agregarMemoriaACriterio(char* numeroMemoria, char* tipoConsistencia) {

	char* copiaNumero = string_duplicate(numeroMemoria);

	bool seEncuentraElMemoryNumber(tabla_Gossiping* memoriaExistente){
		return string_contains(copiaNumero, memoriaExistente->numeroMemoria);
	}

	if(!list_any_satisfy(listaGossiping, (void*) seEncuentraElMemoryNumber)){
		log_error(logger, "No Existe El Memory Number %s Dentro Del Gossip", numeroMemoria);
		free(copiaNumero);
		return;
	}

	int index = 0;
	criterio_Tablas* elementoCriterio = list_get(tablasCriterios, index);

	while (elementoCriterio != NULL) {
		if (string_contains(elementoCriterio->tipoConsistencia,tipoConsistencia)) {
			if (verificarSiEsSCYSoloHayUnaMemoria(elementoCriterio)) {
				agregarSiNoSeEncuentra(copiaNumero, elementoCriterio);
				break;
			}
		}

		index++;
		elementoCriterio = list_get(tablasCriterios, index);
	}
}

void agregarSiNoSeEncuentra(char* numeroMemoria, criterio_Tablas* elementoCriterio){

	int enteroNumeroMemoria = atoi(numeroMemoria);

	bool sonElMismoNumero(tiempo_Memorias* memoriaAComparar){
		int valorEntero = atoi(memoriaAComparar->numeroMemoria);
		return valorEntero == enteroNumeroMemoria;
	}

	if(list_any_satisfy(elementoCriterio->memoriasAsignadas, (void*) sonElMismoNumero)){
		log_error(logger, "Ya Se Encuentra Asociada La Memoria - %s", numeroMemoria);
		return;
	}

	if(string_contains(elementoCriterio->tipoConsistencia, "SHC")){

		criterio_Tablas* criterioSHC = list_get(tablasCriterios, 2);

		int cantidadMemoriasAsociadas = list_size(criterioSHC->memoriasAsignadas);

		for(int i = 0; i < cantidadMemoriasAsociadas; i++){

			tiempo_Memorias* memoriaAsociada = list_get(criterioSHC->memoriasAsignadas, i);

			bool tienenElMismoMemoryNumber(tabla_Gossiping* elemento){
				return string_contains(elemento->numeroMemoria, memoriaAsociada->numeroMemoria);
			}

			tabla_Gossiping* memoriaEncontrada = list_find(listaGossiping, (void *) tienenElMismoMemoryNumber);
			runFunction(memoriaEncontrada->socketMemoria, "realizarSecuenciaJournal", 0);
			sem_wait(&respuestaJournal);
		}
	}

	tiempo_Memorias* memoriaNueva = malloc(sizeof(tiempo_Memorias));
	memoriaNueva->numeroMemoria = numeroMemoria;
	memoriaNueva->contadorInserts = 0;
	memoriaNueva->contadorSelects = 0;

	pthread_mutex_lock(&m_memoria);
	list_add(elementoCriterio->memoriasAsignadas, memoriaNueva);
	pthread_mutex_unlock(&m_memoria);
	log_info(logger, "Se Agrego La Memoria - %s Al Criterio %s", numeroMemoria, elementoCriterio->tipoConsistencia);
}

bool verificarSiEsSCYSoloHayUnaMemoria(criterio_Tablas* criterio) {

	if (string_contains(criterio->tipoConsistencia, "SC")) {

		if (list_size(criterio->memoriasAsignadas) < 1) {
			return 1;
		}
		log_error(logger, "Ya Se Encuentra Asignada Una Memoria Para SC");
		return 0;
	}

	return 1;
}

void crearArchivoLQL(char* unicaLinea){

	char* direccionLocal = "LISSANDRA_FS/Archivos/Scripts/";
	char* nombrePath = "archivoScriptLQL";
	char* direccionCompleta = string_new();
	char* nombreScript = string_new();

	string_append(&direccionCompleta, direccionLocal);
	string_append(&nombreScript, nombrePath);
	string_append_with_format(&nombreScript, "-%d.lql", generadorNumeroLQL);
	string_append(&direccionCompleta, nombreScript);
	generadorNumeroLQL++;
	FILE* punteroArchivo = fopen(direccionCompleta,"w+");
	fwrite(unicaLinea, 1, strlen(unicaLinea), punteroArchivo);
	log_info(logger, "Se Creo El Archivo %s Y Se Escribio %s", nombreScript, unicaLinea);
	fclose(punteroArchivo);

	tiny_pcb* miniEstructuraANew = malloc(sizeof(tiny_pcb));
	miniEstructuraANew->ruta = nombreScript;
	miniEstructuraANew->estado = NEW;
	encolarRutaScript(colaNew, miniEstructuraANew ,m_colaNew);

	free(direccionCompleta);
	sem_post(&entradaProcesosEnNew);
}

void crearEstructuraPCB(char* nombreScript){

	tiny_pcb* miniEstructuraANew = malloc(sizeof(tiny_pcb));
	miniEstructuraANew->ruta = string_duplicate(nombreScript);
	miniEstructuraANew->estado = NEW;
	encolarRutaScript(colaNew, miniEstructuraANew ,m_colaNew);

	sem_post(&entradaProcesosEnNew);
}

//Este Describe Sera Solamente Cuando El Kernel Y Las Memorias Se Conocen
//args[0]: NombreTabla TipoConsistencia CantidadParticiones TiempoCompactacion
void pasaManoDescribe(socket_connection* connection, char** args) {

	if (string_is_empty(args[0])) {
		log_warning(logger, "Todavia No Hay Ninguna Tabla");
		sem_post(&ejecucionDescribe);
	}

	else {
		separarTablasEnUnDescribeGlobal(args[0]);
	}
}

void respuestaSobreDescribeSimple(socket_connection* connection, char** args) {
	respuestaDescribe = atoi(args[0]);
	separarTablasEnUnDescribeGlobal(args[1]);
}

void respuestaSobreGossip(socket_connection* connection, char** args){

	char** listadoMemorias = string_split(args[0], ",");

	int indice = 0;
	while (listadoMemorias[indice] != NULL) {
		char** tablaComponentes = string_n_split(listadoMemorias[indice], 4,
				" \n\t");
		tabla_Gossiping* tablaRecibida = malloc(sizeof(tabla_Gossiping));
		tablaRecibida->numeroMemoria = string_duplicate(tablaComponentes[0]);
		tablaRecibida->ipMemoria = string_duplicate(tablaComponentes[1]);
		tablaRecibida->puertoMemoria = atoi(tablaComponentes[2]);
		agregarMemoriaAListaDeGossiping(tablaRecibida);
		string_iterate_lines(tablaComponentes, (void*) free);
		free(tablaComponentes);
		indice++;
	}

	string_iterate_lines(listadoMemorias, (void*) free);
	free(listadoMemorias);
	sem_post(&esperaRespuesta);
}

void agregarMemoriaAListaDeGossiping(tabla_Gossiping* memoriaNueva){

	if(list_is_empty(listaGossiping)){

		memoriaNueva->socketMemoria = socketMemoria;
		list_add(listaGossiping, memoriaNueva);
		log_info(logger, "Se Agrego La Primer Memoria Al Gossip");
		return;
	}

	int numeroMemoriaInt = atoi(memoriaNueva->numeroMemoria);

	bool sonElMismoNumero(tabla_Gossiping* memoriaNueva){
		int valorEntero = atoi(memoriaNueva->numeroMemoria);
		return valorEntero == numeroMemoriaInt;
	}

	tabla_Gossiping* memoriaEncontrada = list_find(listaGossiping, (void*) sonElMismoNumero);

	if(memoriaEncontrada != NULL){
		log_error(logger, "El Numero De Memoria %s Ya Existe", memoriaNueva->numeroMemoria);
		free(memoriaNueva->ipMemoria);
		free(memoriaNueva->numeroMemoria);
		free(memoriaNueva);
		return;
	}

	int socketAConectar = connectServer(memoriaNueva->ipMemoria, memoriaNueva->puertoMemoria, llamadaFunciones, &disconnect, NULL);
	runFunction(socketAConectar, "conocerAlKernel", 1, "Kernel");

	if(socketAConectar == -1){
		log_error(logger, "No Se Pudo Conectar Al Ip: %s - Con Puerto: %d", memoriaNueva->ipMemoria, memoriaNueva->puertoMemoria);
		free(memoriaNueva->ipMemoria);
		free(memoriaNueva->numeroMemoria);
		free(memoriaNueva);
		return;
	}

	memoriaNueva->socketMemoria = socketAConectar;
	list_add(listaGossiping, memoriaNueva);
	log_info(logger, "Se Agrego El Memory Number %s Con Socket %d A La Lista", memoriaNueva->numeroMemoria, memoriaNueva->socketMemoria);
	printf("Se Agrego El Memory Number %s Con Socket %d A La Lista\n", memoriaNueva->numeroMemoria, memoriaNueva->socketMemoria);
}

void respuestaDescribeDeUnaTabla(socket_connection* connection, char** args){
	respuestaDescribe = atoi(args[0]);

	tables_Metadata* tablaRecibida = malloc(sizeof(tables_Metadata));
	tablaRecibida->nombreTabla = string_duplicate(args[1]);
	tablaRecibida->tipoConsistencia = string_duplicate(args[2]);
	tablaRecibida->cantidadParticiones = atoi(args[3]);
	tablaRecibida->tiempoCompactacion = atoi(args[4]);
	agregarTabla(tablaRecibida);
	sem_post(&ejecucionDescribe);
}

void separarTablasEnUnDescribeGlobal(char* listaTablas) {

	char** listadoTablas = string_split(listaTablas, ",");

	int indice = 0;
	while (listadoTablas[indice] != NULL) {
		char** tablaComponentes = string_n_split(listadoTablas[indice], 4,
				" \n\t");
		tables_Metadata* tablaRecibida = malloc(sizeof(tables_Metadata));
		tablaRecibida->nombreTabla = string_duplicate(tablaComponentes[0]);
		tablaRecibida->tipoConsistencia = string_duplicate(tablaComponentes[1]);
		tablaRecibida->cantidadParticiones = atoi(tablaComponentes[2]);
		tablaRecibida->tiempoCompactacion = atoi(tablaComponentes[3]);
		agregarTabla(tablaRecibida);
		string_iterate_lines(tablaComponentes, (void*) free);
		free(tablaComponentes);
		indice++;
	}

	string_iterate_lines(listadoTablas, (void*) free);
	free(listadoTablas);
	sem_post(&ejecucionDescribe);
}

void finalizarHilosActivos(){

	sem_post(&entradaProcesosEnNew);
	sem_post(&entradaProcesosEnReady);

	int i = 0;
	execute_t* hiloExec = list_get(listaExecutes,i);

	while(i < datosConfigKernel->multiProcesamiento){
		sem_post(&hiloExec->enEspera);
		i++;
		hiloExec = list_get(listaExecutes,i);
	}
}

void esperaJoinDeLosHilosDeMultiprocesamiento(){

	int i = 0;
	execute_t* hiloExec = list_get(listaExecutes,i);

	while(i < datosConfigKernel->multiProcesamiento){
		pthread_join(hiloExec->idHiloExecute, NULL);
		i++;
		hiloExec = list_get(listaExecutes,i);
	}
}

bool esTiempoDeEjecutarse(unsigned inicioTiempo, unsigned nuevoTiempo, int tiempoACumplir) {
	return (nuevoTiempo - inicioTiempo) == tiempoACumplir;
}

void planificarGossip() {

	int tiempoGossipMecanico = datosConfigKernel->tiempoGossiping;
	int valorRetardo = datosConfigKernel->sleepEjecucion;

	while (flagSalida) {

		sleep(tiempoGossipMecanico / 1000);
		pthread_mutex_lock(&iNotify);
		if (tiempoGossipMecanico != datosConfigKernel->tiempoGossiping) {
			tiempoGossipMecanico = datosConfigKernel->tiempoGossiping;
		}
		pthread_mutex_unlock(&iNotify);

		pthread_mutex_lock(&m_memoriaPrincipal);

		log_info(logger, "Se Iniciara El Gossip");
		if (runFunction(socketMemoria, "realizarGossipConMemoriaPrincipal", 1,
				"Gossip")) {
			sem_wait(&esperaRespuesta);
		}

		else {
			socketMemoria = connectServer(datosConfigKernel->ipMemoria,
					datosConfigKernel->puertoMemoria, llamadaFunciones,
					&disconnect, NULL);
			if (socketMemoria != -1) {
				runFunction(socketMemoria, "identificarProcesoConMemoria", 1,
						"Kernel");
				sem_wait(&esperaRespuesta);
			}
		}

		pthread_mutex_unlock(&m_memoriaPrincipal);
	}
	log_trace(logger, "Finalizo Hilo De Gossiping");
	printf("Finaliza Hilo De Gossiping");
}

void planificarDescribeGlobal(){

	unsigned inicioDeGossip = (unsigned) time(NULL);
	int tiempoDescribeMecanico = datosConfigKernel->metadataRefresh;
	int valorRetardo = datosConfigKernel->sleepEjecucion;
	int contadorDeDescribe = 1;

	while(flagSalida){

		sleep(valorRetardo);
		pthread_mutex_lock(&iNotify);
		if(tiempoDescribeMecanico != datosConfigKernel->metadataRefresh){
			tiempoDescribeMecanico = datosConfigKernel->metadataRefresh;
		}
		pthread_mutex_unlock(&iNotify);

		unsigned nuevoTiempo = (unsigned) time(NULL);
		int cantVecesGossipPorTiempo = contadorDeDescribe * tiempoDescribeMecanico;
		if (esTiempoDeEjecutarse(inicioDeGossip, nuevoTiempo,
				cantVecesGossipPorTiempo)) {
			int eleccionMemoria = rand() % list_size(listaGossiping);
			tabla_Gossiping* memoriaSeleccionada = list_get(listaGossiping,
					eleccionMemoria);
			log_trace(logger,
					"Se Enviara Un Describe Global Al Memory Number: %s Con Socket: %d",
					memoriaSeleccionada->numeroMemoria,
					memoriaSeleccionada->socketMemoria);
			runFunction(memoriaSeleccionada->socketMemoria, "identificarProcesoConDescribe",
					1, "Kernel");
				contadorDeDescribe++;
				sem_wait(&esperaRespuesta);
			}
		}
		log_trace(logger,"Finalizo Hilo De Describe's");
	}

void liberarElementosDeTabla(tables_Metadata* tablaAEliminar){

	free(tablaAEliminar->nombreTabla);
	free(tablaAEliminar->tipoConsistencia);
	free(tablaAEliminar);
}

void logueoDeMetricasManual(){

	double totalTiempoSelects;
	double totalTiempoInserts;
	double totalCantidadSelects;
	double totalCantidadInserts;

	if (list_size(listaGossiping) == 0) {
		log_error(logger, "No Hay Memorias Que Kernel Conozca");
		return;
	}

	else {
		criterio_Tablas* criterioSC = list_get(tablasCriterios, 0);
		criterio_Tablas* criterioEC = list_get(tablasCriterios, 1);
		criterio_Tablas* criterioSHC = list_get(tablasCriterios, 2);

		if (list_is_empty(criterioSC->memoriasAsignadas) && list_is_empty(criterioEC->memoriasAsignadas) && list_is_empty(criterioSHC->memoriasAsignadas)){
			log_error(logger, "No Hay Memorias Asignadas Para Calcular Sus Metricas");
			return;
		}
	}

	t_list* memoriasAsociadasATrabajo = list_create();

	pthread_mutex_lock(&m_memoria);
	for(int k = 0; k < list_size(tablasCriterios); k++){

		criterio_Tablas* criterio = list_get(tablasCriterios, k);
		totalTiempoInserts += criterio->tiempoInserts;
		totalTiempoSelects += criterio->tiempoSelects;
	}

	for(int i = 0; i < list_size(listaGossiping); i++){

		tabla_Gossiping* memoria = list_get(listaGossiping, i);
		int contadorTotalInserts, contadorTotalSelects;

		for(int j = 0; j < list_size(tablasCriterios); j++){

			criterio_Tablas* criterioAsociado = list_get(tablasCriterios, j);

			bool tienenElMismoMemoryNumber(tiempo_Memorias* memoriaCriterio){
				return string_contains(memoriaCriterio->numeroMemoria, memoria->numeroMemoria);
			}

			tiempo_Memorias* memoriaAsociada = list_find(criterioAsociado->memoriasAsignadas, (void*) tienenElMismoMemoryNumber);

			if(memoriaAsociada != NULL){
				contadorTotalInserts += memoriaAsociada->contadorInserts;
				contadorTotalSelects += memoriaAsociada->contadorSelects;
				totalCantidadInserts += memoriaAsociada->contadorInserts;
				totalCantidadSelects += memoriaAsociada->contadorSelects;
			}
		}

		tiempo_Memorias* memoriaContador = malloc(sizeof(tiempo_Memorias));
		memoriaContador->numeroMemoria = string_duplicate(memoria->numeroMemoria);
		memoriaContador->contadorInserts = contadorTotalInserts;
		memoriaContador->contadorSelects = contadorTotalSelects;
		list_add(memoriasAsociadasATrabajo, memoriaContador);
	}
	pthread_mutex_unlock(&m_memoria);

	log_info(logger, "Tiempo Promedio SELECT = %.2f", totalTiempoSelects/totalCantidadSelects);
	log_info(logger, "Tiempo Promedio INSERT = %.2f", totalTiempoInserts/totalCantidadInserts);
	log_info(logger, "Cantidad De SELECT = %.0f", totalCantidadSelects);
	log_info(logger, "Cantidad De INSERT = %.0f", totalCantidadInserts);

	pthread_mutex_lock(&m_operacionesTotales);
	for(int n = 0; n < list_size(memoriasAsociadasATrabajo); n++){

		tiempo_Memorias* memoriaCantidad = list_get(memoriasAsociadasATrabajo, n);
		double contadorTotalMemoria = (memoriaCantidad->contadorInserts + memoriaCantidad->contadorSelects) / operacionesTotales;
		log_info(logger, "Memory Load Number - %s: Cantidad INSERT/SELECT = %.2f", memoriaCantidad->numeroMemoria, ceil(contadorTotalMemoria));
	}
	pthread_mutex_unlock(&m_operacionesTotales);

	list_clean_and_destroy_elements(memoriasAsociadasATrabajo, (void*) eliminarMemoriasContador);
	list_destroy(memoriasAsociadasATrabajo);
}

void eliminarMemoriasContador(tiempo_Memorias* memoria){
	free(memoria->numeroMemoria);
	free(memoria);
}