#include <stdio.h>
#include <stdlib.h>
#include "libMemoria.h"

//Creacion Del Archivo Para Logs
void configure_logger() {

	char * nombrePrograma = "Memoria.log";
	char * nombreArchivo = "Memoria";

	logger = log_create(nombrePrograma, nombreArchivo, false, LOG_LEVEL_TRACE);
	log_info(logger, "Se genero log de Memoria");
}

t_config_Memoria * read_and_log_config(char* path) {

	char* ipFS;
	int* puertosSemilla;
	char** numbers_expected;
	char** ips_expected;
	char* memoryNumber;

	log_info(logger, "Voy a leer el archivo memoria.config");

	t_config* archivo_Config = config_create(path);
	if (archivo_Config == NULL) {
		log_error(logger, "No existe archivo de configuracion");
		exit(1);
	}

	datosConfigMemoria = malloc(sizeof(t_config_Memoria));

	int puertoPropio = config_get_int_value(archivo_Config, "PUERTO");
	datosConfigMemoria->puertoMemoria = puertoPropio;

	ipFS = string_new();
	string_append(&ipFS, config_get_string_value(archivo_Config, "IP_FS"));
	datosConfigMemoria->ipFileSystem = ipFS;

	int puertoFS = config_get_int_value(archivo_Config, "PUERTO_FS");
	datosConfigMemoria->puertoFS = puertoFS;

	ips_expected = config_get_array_value(archivo_Config, "IP_SEEDS");
	datosConfigMemoria->ipVectorMemorias = ips_expected;

	numbers_expected = config_get_array_value(archivo_Config, "PUERTO_SEEDS");
	datosConfigMemoria->puertosSeeds = numbers_expected;

	datosConfigMemoria->retardoMemoria = (config_get_int_value(archivo_Config,
			"RETARDO_MEM") / 1000);
	datosConfigMemoria->retardoFS = (config_get_int_value(archivo_Config,
			"RETARDO_FS") / 1000);
	datosConfigMemoria->tamanioMemoria = config_get_int_value(archivo_Config,
			"TAM_MEM");
	datosConfigMemoria->retardoJournal = config_get_int_value(archivo_Config,
			"RETARDO_JOURNAL");
	datosConfigMemoria->retardoGossiping = config_get_int_value(archivo_Config,
			"RETARDO_GOSSIPING");
	memoryNumber = string_new();
	string_append(&memoryNumber,
			config_get_string_value(archivo_Config, "MEMORY_NUMBER"));
	datosConfigMemoria->memoryNumber = memoryNumber;

	char* ipsConjuntos = mostrarConfiguracionConexion(ips_expected);
	char* puertosConjuntos = mostrarConfiguracionConexion(numbers_expected);

	log_info(logger, "PUERTO: %d", datosConfigMemoria->puertoMemoria);
	log_info(logger, "IP_FS: %s", datosConfigMemoria->ipFileSystem);
	log_info(logger, "PUERTO_FS: %d", datosConfigMemoria->puertoFS);
	log_info(logger, "IP_SEEDS: [%s]", ipsConjuntos);
	log_info(logger, "PUERTO_SEEDS: [%s]", puertosConjuntos);
	log_info(logger, "RETARDO_MEM: %d", datosConfigMemoria->retardoMemoria);
	log_info(logger, "RETARDO_FS: %d", datosConfigMemoria->retardoFS);
	log_info(logger, "TAM_MEM: %u", datosConfigMemoria->tamanioMemoria);
	log_info(logger, "RETARDO_JOURNAL: %d", datosConfigMemoria->retardoJournal);
	log_info(logger, "RETARDO_GOSSIPING: %d",
			datosConfigMemoria->retardoGossiping);
	log_info(logger, "MEMORY_NUMBER: %s", datosConfigMemoria->memoryNumber);

	log_info(logger, "Fin de lectura");

	config_destroy(archivo_Config);
	free(ipsConjuntos);
	free(puertosConjuntos);
	return datosConfigMemoria;
}

//Cierre Del Archivo De Logs
void close_logger() {
	log_info(logger, "Cierro log de Memoria");
	log_destroy (logger);
}

char* mostrarConfiguracionConexion(char** conexiones) {

	int cantidadConexiones = retornarCantidadDeBlocksEnParticion(conexiones);

	if (cantidadConexiones == 0) {
		return NULL;
	}

	char* elementosConjuntos = string_new();

	for (int i = 0; i < cantidadConexiones; i++) {

		if (i == 0) {
			string_append_with_format(&elementosConjuntos, "%s", conexiones[i]);
		} else {
			string_append_with_format(&elementosConjuntos, ", %s",
					conexiones[i]);
		}
	}
	return elementosConjuntos;
}

int retornarCantidadDeBlocksEnParticion(char** listaBloques) {
	int cantidadElementos = 0;
	while (listaBloques[cantidadElementos] != NULL) {
		cantidadElementos++;
	}
	return cantidadElementos;
}

void identificarConKernel(socket_connection* connection, char** args) {

	if (string_contains(args[0], "Kernel")) {
		socketKernel = connection->socket;
		log_info(logger,
				"Se Ha Conectado: %s En El Socket NRO: %d Con IP:%s, PUERTO:%d",
				args[0], connection->socket, connection->ip, connection->port);
	}

	char* listaMemorias = enviarTablaGossipEnUno();
	runFunction(socketKernel, "devolverListaGossip", 1, listaMemorias);
	free(listaMemorias);
}

void agregarAlKernel(socket_connection* connection, char** args) {
	socketKernel = connection->socket;
	log_info(logger,
			"Se Ha Conectado: %s En El Socket NRO: %d Con IP:%s, PUERTO:%d",
			args[0], connection->socket, connection->ip, connection->port);
}

void identificarProcesoALFS(socket_connection* connection, char** args) {
	log_info(logger, "El %s Me Solicito Un Describe A LFS", args[0]);
	runFunction(socketLFS, "realizarDescribeConLFS", 0); //el describe no recibe parametros, por eso se pasa "0"
}

//args[0]: Tamanio Value
void obtenerTamanioPalabra(socket_connection* connection, char** args) {
	tamanioValue = atoi(args[0]);
	log_info(logger, "Me Llego Que Los Values Son De Tamanio %d", tamanioValue);
	sem_post (&handshakeValue);
}

void pasarDescribeALFS(socket_connection* connection, char** args) {
	log_trace(logger, "Me Llego Una Sentencia Describe");
	if (!string_contains(args[0], "GLOBAL")) {
		runFunction(socketLFS, "realizarDefinidoDescribeConLFS", 1, args[0]);
	} else {
		runFunction(socketLFS, "realizarDefinidoDescribeConLFS", 1, args[0]);
	}
}

void pasarDescribeAlIdentificarse(socket_connection* connection, char** args) {
	log_trace(logger, "Recibi Un Describe Al Identificarse El Kernel");
	runFunction(socketKernel, "informacionSobreTablas", 1, args[0]);
}

void pasarDescribeGlobalEnUno(socket_connection* connection, char** args) {
	runFunction(socketKernel, "respuestaDescribeEnUnoDeLFS", 2, args[0],
			args[1]);
}

void pasaManoCreate(socket_connection* connection, char** args) {
	log_trace(logger, "Recibi Un Create");
	sleep(datosConfigMemoria->retardoFS);
	runFunction(socketLFS, "realizarCreateEnLFS", 4, args[0], args[1], args[2],
			args[3]);
}

void pasaManoSelect(socket_connection* connection, char** args) {
	log_trace(logger, "Recibi Un Select");
	pthread_t hiloEsperaSelect;
	SELECT_MEMORIA_t* select_struct = malloc(sizeof(SELECT_MEMORIA_t));
	select_struct->tabla = string_duplicate(args[0]);
	select_struct->key = string_duplicate(args[1]);
	pthread_create(&hiloEsperaSelect, NULL, (void*) SELECT_MEMORIA,
			select_struct);
	pthread_detach(hiloEsperaSelect);
}

void insertarOActualizarKey(socket_connection* connection, char** args) {
	pthread_t hiloEspera;
	INSERT_MEMORIA_t * insert_mem_struct = malloc(sizeof(INSERT_MEMORIA_t));
	insert_mem_struct->tabla = string_duplicate(args[0]);
	insert_mem_struct->key = string_duplicate(args[1]);
	insert_mem_struct->valor = string_duplicate(args[2]);
	insert_mem_struct->timestamp = getCurrentTime();
	insert_mem_struct->connection = connection;

	pthread_create(&hiloEspera, NULL, (void*) INSERT_MEMORIA,
			insert_mem_struct);
	pthread_detach(hiloEspera);
	sleep(datosConfigMemoria->retardoMemoria);
}

void pasaManoDrop(socket_connection* connection, char** args) {
	char* tablaParaDropear = string_duplicate(args[0]);
	log_trace(logger, "Recibi Un Drop Para %s", tablaParaDropear);
	DROP_MEMORIA(tablaParaDropear, connection);
	free(tablaParaDropear);
}

void enviarUltimoRegistro(socket_connection* connection, char** args) {

	bool resultadoOk_LFS;
	char* resultadoLFS;
	char* tabla;
	char* resultadoParaTS;
	char* key;
	char* valor;

	if (string_contains(args[0], "0")) {
		resultadoOk_LFS = false;
		tabla = string_duplicate(args[1]);
		key = string_duplicate(args[2]);

		log_error(logger, "Error en LFS. No Se Encontro La Key %s En La %s.",
				key, tabla);
		runFunction(socketKernel, "registroUltimoObtenido", 3, args[0], tabla,
				key);
		free(tabla);
		free(key);
	}

	else {
		resultadoOk_LFS = true;

		resultadoLFS = string_duplicate(args[1]); //ts key value
		tabla = string_duplicate(args[2]);

		resultadoParaTS = strtok(resultadoLFS, ";");
		key = strtok(NULL, ";");
		valor = strtok(NULL, ";");

		INSERT_MEMORIA_t * insert_mem_struct = malloc(sizeof(INSERT_MEMORIA_t));
		insert_mem_struct->tabla = tabla;
		insert_mem_struct->key = key;
		insert_mem_struct->valor = valor;
		insert_mem_struct->timestamp = getCurrentTime();
		insert_mem_struct->connection = NULL;

		//bitarrayImprimir1eros100();

		pthread_t hiloParaInsert;
		pthread_create(&hiloParaInsert, NULL, (void*) SEL_INS_MEM,
				insert_mem_struct);
		pthread_detach(hiloParaInsert);

		log_info(logger, "Resultado: SELECT %s %s %s", tabla, key, valor);
		runFunction(socketKernel, "registroUltimoObtenido", 3, args[0], args[1], args[2]);
	}
}

//Otra Funcion Fuera De enviarUltimoRegistro
void SEL_INS_MEM(INSERT_MEMORIA_t* insert_struct) {

	nodoPagina_t* paginaInsertada = NULL;

	paginaInsertada = INSERT_MEMORIA(insert_struct);

	if (paginaInsertada == NULL) {
		log_error(logger, "Error en LFS. Tabla %s o key %s inexistente.", insert_struct->tabla,
				insert_struct->key);
		return;
	}
	paginaInsertada->flags = false;
	//free(insert_mem_struct);
}

void devolverDescribeSimple(socket_connection* connection, char** args) {
	log_trace(logger, "Enviando Describe De La %s %s %s %s", args[1], args[2],
			args[3], args[4]);
	runFunction(socketKernel, "respuestaDescribeSimple", 5, "1", args[1],
			args[2], args[3], args[4]);
}

//Funcion Solamente Para Las Respuestas De CREATE-INSERT-DROP
//args[0]: Sentencia ,args[1]: Respuesta Por "-1"/"0"/"1
void devolverRespuesta(socket_connection* connection, char** args) {

	if (string_contains(args[0], "INSERT")) {
		log_info(logger, "Recibi Un Insert");
		sem_post (&semInserts);
		return;
	}

	runFunction(socketKernel, "respuestaDeLFS", 2, args[0], args[1]);
}

void inicializarMemoria() {
	//Iniciando La Estructura Del Servidor
	socketLFS = connectServer(datosConfigMemoria->ipFileSystem,
			datosConfigMemoria->puertoFS, llamadaFunciones, &disconnect, NULL);

	if (socketLFS == -1) {
		log_error(logger,
				"No Se Pudo Conectar Con LFS - Finalizando El Programa");
		exit(0);
	}
	log_info(logger, "Me Conecte A LFS");
	runFunction(socketLFS, "identificarProcesoMemoria", 1, "Memoria");
	sem_wait (&handshakeValue);

	pMemoriaPrincipal = malloc(datosConfigMemoria->tamanioMemoria); //inicializar memoria
	memset(pMemoriaPrincipal, '\0', datosConfigMemoria->tamanioMemoria);
	tablaDeSegmentos = list_create(); //inicializar tabla de segmentos

	tamanioDeMarco = sizeof(double) + sizeof(uint16_t) + tamanioValue; // timestamp + key + value

	printf("     tamanioDeMarco: %u \n", tamanioDeMarco);
	printf("            TAM_MEM: %u \n", datosConfigMemoria->tamanioMemoria);
	printf("    cantidad marcos: %d \n",
			datosConfigMemoria->tamanioMemoria / tamanioDeMarco);

	char* marcosTotales = malloc(
			datosConfigMemoria->tamanioMemoria / tamanioDeMarco);
	cantidadMarcos = (int) floor(
			datosConfigMemoria->tamanioMemoria / tamanioDeMarco);
	printf("cantidad marcos: %d\n", cantidadMarcos);

	if (cantidadMarcos < 8) {
		bitarrayMarcos = bitarray_create_with_mode(marcosTotales, 1, LSB_FIRST);
	} else {
		bitarrayMarcos = bitarray_create_with_mode(marcosTotales,
				(datosConfigMemoria->tamanioMemoria / tamanioDeMarco) / 8,
				LSB_FIRST);
	}

	limpiarMarcos();

	//inicializa gossiping
	listaDeGossiping = list_create();
}

int validarTamanioValue(char* value) {
	if (strlen(value) <= tamanioValue)
		return FN_SUCCESS;
	else
		return FN_ERROR;
}

nodoSegmento_t* crearTablaSegmento(char* nombreDeLaTabla) {
	nodoSegmento_t* nuevoNodoSegmento;

	nuevoNodoSegmento = malloc(sizeof(nodoSegmento_t));
	nuevoNodoSegmento->nombreDeLaTabla = copiarStr(nombreDeLaTabla);
	nuevoNodoSegmento->tablaDePaginas = list_create();

	list_add(tablaDeSegmentos, nuevoNodoSegmento);

	return nuevoNodoSegmento;
}

tipoNroPagina buscarMarcoLibreEnMemoria() {
	for (tipoNroPagina i = 0; i < cantidadMarcos; i++) {
		//printf("buscar bit array: %u\n", i);
		pthread_mutex_lock (&m_bitarray);
		if (bitarray_test_bit(bitarrayMarcos, i) == false) {
			bitarray_set_bit(bitarrayMarcos, i);
			pthread_mutex_unlock(&m_bitarray);
			return i;
		}
	}
	pthread_mutex_unlock (&m_bitarray);
	return 0;
}
void bitarrayImprimir1eros100() {
	printf("==================bit array==================\n");
	for (tipoNroPagina i = 0; i < cantidadMarcos && i < 100; i++) {
		printf("%u", bitarray_test_bit(bitarrayMarcos, i));
	}
	printf("\n=============================================\n");
}

//int yaExisteLaTabla(char *nombreDeLaTabla)
//{
//	bool bfnSegmento(nodoSegmento_t* segmento)
//	{
//		nodoSegmento_t* unSegmento = segmento;
//		return strcmp(unSegmento->nombreDeLaTabla, nombreDeLaTabla) == 0;
//	}
//
//	//TODO pthread_mutex_lock(&mutex);
//	if( list_find(tablaDeSegmentos, (void*) bfnSegmento) != NULL )
//	{
//		return FN_SUCCESS;
//	}
//	else
//	{
//		return FN_ERROR;
//	}
//}

nodoSegmento_t* buscarSegmentoPorNombreDeTabla(char *nombreDeLaTabla) {
	bool bfnSegmento(void* segmento)
	{
		nodoSegmento_t* unSegmento = (nodoSegmento_t*) segmento;
		return strcmp(unSegmento->nombreDeLaTabla, nombreDeLaTabla) == 0;
	}

	if(list_size(tablaDeSegmentos) == 0){
				return NULL;
			}

	return list_find(tablaDeSegmentos, (void *) bfnSegmento);
}

bool tamanioKeyMayorQueElMaximo(int keyTamanio) {
	uint16_t valorMaximo = 0xFFFF;
	//printf("tamanio %d  %d\n",keyTamanio,valorMaximo);
	return keyTamanio > valorMaximo;
}

char* copiarStr(const char * palabra) {
	char * tmp = malloc(sizeof(char) * (strlen(palabra) + 1));
	memcpy(tmp, palabra, strlen(palabra));
	tmp[strlen(palabra)] = '\0';
	return tmp;
}

void liberarMemoria() {
	free (pMemoriaPrincipal);

	list_destroy (tablaDeSegmentos); // TODO realizar list_destroy_and_destroy_elements
}

void controladorJournal() {
	int tiempoJournal = (datosConfigMemoria->retardoJournal / 1000);

	log_info(logger, "Se Creo El Hilo Del Journal");

	while (flagSalida) {
		sleep(tiempoJournal);
		log_info(logger, "Comenzar proceso de Journal");
		JOURNAL_MEMORIA (NULL);
	}

	log_info(logger, "Se Cierra Hilo De Journal");
}

void* notificarEventos(char* path) {

	char buffer[BUF_LEN];

	int file_descriptor = inotify_init();
	if (file_descriptor < 0) {
		log_error(logger, "An Error Occur With INotify_Init");
		return NULL;
	}

	int watch_descriptor = inotify_add_watch(file_descriptor, path, IN_MODIFY);

	while (flagSalida) {

		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0) {
			log_error(logger, "Error Trying To Use Read");
			inotify_rm_watch(file_descriptor, watch_descriptor);
			close(file_descriptor);
			return NULL;
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
						sem_wait (&iNotify);
						char* copiaPath = string_new();
						string_append(&copiaPath, path);
						string_append(&copiaPath, "/memoria.config");
						log_info(logger, "The file %s was modified.\n",
								event->name);
						t_config* archivo_Config = config_create(copiaPath);
						datosConfigMemoria->retardoMemoria =
								config_get_int_value(archivo_Config,
										"RETARDO_MEM");
						datosConfigMemoria->retardoFS = config_get_int_value(
								archivo_Config, "RETARDO_FS");
						datosConfigMemoria->retardoJournal =
								config_get_int_value(archivo_Config,
										"RETARDO_JOURNAL");
						datosConfigMemoria->retardoGossiping =
								config_get_int_value(archivo_Config,
										"RETARDO_GOSSIPING");
						config_destroy(archivo_Config);
						free(copiaPath);
						sem_post(&iNotify);
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
	return NULL;
}

void SELECT_MEMORIA(SELECT_MEMORIA_t* select_struct,
		socket_connection* connection) {
	string_to_upper(select_struct->tabla);

	pthread_mutex_lock (&m_segmento);
	nodoSegmento_t* nodoSegmentoTabla = buscarSegmentoPorNombreDeTabla(
			select_struct->tabla);

	if (nodoSegmentoTabla != NULL) // Si encuentra la tabla en memoria
			{
		if ((int) obtenerPaginaEnMemoria(select_struct->tabla,
				nodoSegmentoTabla->tablaDePaginas, select_struct->key,
				connection) == -1) //Si no lo encuentra en MP
				{
			SELECT_LFS(select_struct->tabla, select_struct->key);
		}

	} else {  // Si no encuentra la tabla en memoria la va ir a buscar en LFS
		SELECT_LFS(select_struct->tabla, select_struct->key);

	}
	pthread_mutex_unlock(&m_segmento);
}

tipoNroPagina obtenerPaginaEnMemoria(char* tabla, t_list* listaPaginas,
		char* key, socket_connection* connection) {
	nodoPagina_t* nodoDeUnaPagina = NULL;
	char * valor = NULL;

	uint16_t keyUint16 = (uint16_t) atoi(key);

	uint16_t keyUint16aux;

	for (int i = 0; i < list_size(listaPaginas); i++) {
		nodoDeUnaPagina = list_get(listaPaginas, i);

		memcpy(&keyUint16aux,
				pMemoriaPrincipal
						+ (nodoDeUnaPagina->nroPagina * tamanioDeMarco),
				sizeof(uint16_t)); // copio Key desde memoria

		if (keyUint16aux == keyUint16) {
			valor = malloc(tamanioValue + 1);
			memset(valor, '\0', tamanioValue + 1);
			memcpy(valor,
					pMemoriaPrincipal
							+ (nodoDeUnaPagina->nroPagina * tamanioDeMarco)
							+ sizeof(uint16_t) + sizeof(double), tamanioValue); // copio valor desde la memoria
			valor[tamanioValue] = '\0';

			double timestamp;
			memcpy(&timestamp,
					pMemoriaPrincipal
							+ (nodoDeUnaPagina->nroPagina * tamanioDeMarco)
							+ sizeof(uint16_t), sizeof(double)); //obtener timestamp de MP

			if (connection != NULL) {
				log_info(logger,
						"Devolver Al Kernel Si Los Conozco %s %s --> valor: %s timestamp: %f",
						tabla, key, valor, nodoDeUnaPagina->timeStamp);
				char* registroObtenido = string_new();
				char* output = malloc(14 * sizeof(char));
				snprintf(output, 14, "%.f", timestamp);

				//char* timeStamp = string_itoa(nodoDeUnaPagina->timeStamp);

				string_append_with_format(&registroObtenido, "%s;%s;%s", output,
						key, valor);
				runFunction(socketKernel, "registroUltimoObtenido", 3, "1",
						registroObtenido, tabla);
				log_info(logger, "Se Envio El Select");
				free(output);
				free(registroObtenido);
				pthread_mutex_unlock (&m_journal);

			}
			free(valor);
			//nodoDeUnaPagina->timeStamp = getCurrentTime();
			return nodoDeUnaPagina->nroPagina;
		}
	}
	return -1;
}

marco_t* obtenerRegistroDesdeMemoria(nodoPagina_t* nodoDeUnaPagina) {
	char * valor;
	marco_t* marco = malloc(sizeof(marco_t));
	uint16_t keyUint16;
	double timestamp;
	char* output = malloc(14 * sizeof(char));

	memcpy(&timestamp,
			pMemoriaPrincipal + (nodoDeUnaPagina->nroPagina * tamanioDeMarco)
					+ sizeof(uint16_t), sizeof(double)); // copio timestamp desde la memoria
	memcpy(&keyUint16,
			pMemoriaPrincipal + (nodoDeUnaPagina->nroPagina * tamanioDeMarco),
			sizeof(uint16_t)); // copio Key desde memoria

	valor = malloc(tamanioValue + 1);
	memset(valor, '\0', tamanioValue + 1);
	memcpy(valor,
			pMemoriaPrincipal + (nodoDeUnaPagina->nroPagina * tamanioDeMarco)
					+ sizeof(uint16_t) + sizeof(double), tamanioValue); // copio valor desde la memoria
	valor[tamanioValue] = '\0';

	log_info(logger,
			"Ver Si Tengo El Valor En Memoria %d --> valor: %s timestamp: %f",
			keyUint16, valor, timestamp);
	//TODO devolver al kernel el valor
	nodoDeUnaPagina->timeStamp = getCurrentTime();

	marco->value = valor;
	marco->key = string_itoa(keyUint16);

	snprintf(output, 14, "%f", timestamp);
	marco->timestamp = output;

	return marco;
}

void SELECT_LFS(char* tabla, char* key) {
	sleep(datosConfigMemoria->retardoFS);
	runFunction(socketLFS, "realizarSelectEnLFS", 2, tabla, key);
	log_info(logger, "Se enviaron los datos.");
}

nodoPagina_t* INSERT_MEMORIA(INSERT_MEMORIA_t * insert_mem_st) {
	uint16_t keyUint16 = (uint16_t) atoi(insert_mem_st->key);

	if (tamanioKeyMayorQueElMaximo(keyUint16)) {
		log_error(logger, "Tamanio key %d mayor al maximo.", keyUint16);
		//TODO retornar algo
		return NULL;
	}

	if (!validarTamanioValue(insert_mem_st->valor)) {
		log_error(logger, "El valor se excede al tamanio maximo: %s",
				insert_mem_st->valor);
		//TODO retornar algo
		return NULL;
	}

	pthread_mutex_lock(&m_segmento);
	nodoSegmento_t* segmentoTabla = buscarSegmentoPorNombreDeTabla(
			insert_mem_st->tabla);
	if (segmentoTabla == NULL) {
		segmentoTabla = crearTablaSegmento(insert_mem_st->tabla);
	}
	pthread_mutex_unlock(&m_segmento);

	nodoPagina_t* paginaInsertadaOActualizada;

	paginaInsertadaOActualizada = insertaOActualizaPagina(segmentoTabla,
			insert_mem_st->tabla, insert_mem_st->key, insert_mem_st->valor,
			insert_mem_st->timestamp, insert_mem_st->connection);

	if (insert_mem_st->connection != NULL) {

		sleep(datosConfigMemoria->retardoMemoria);
		runFunction(insert_mem_st->connection->socket, "respuestaDeLFS", 2,
				"INSERT", "1");
	}

	return paginaInsertadaOActualizada;
}

nodoPagina_t* insertaOActualizaPagina(nodoSegmento_t* segmentoTabla,
		char* nombreTabla, char* keyString, char* value, double timestamp,
		socket_connection* connection) {
	nodoPagina_t* paginaAActualizar = NULL;
	uint16_t keyUint16 = (uint16_t) atoi(keyString);

	bool bfnPagina(nodoPagina_t* nodoPagina)
	{
		bool encontro = false;
		uint16_t* keyOnFrame = malloc(sizeof(uint16_t));
		memcpy(keyOnFrame, pMemoriaPrincipal + (nodoPagina->nroPagina * tamanioDeMarco), sizeof(uint16_t));

		if (*keyOnFrame == keyUint16)
		{
			encontro = true;
		}

		free(keyOnFrame);
		return encontro;
	}
	pthread_mutex_lock (&m_pagina);
	paginaAActualizar = list_find(segmentoTabla->tablaDePaginas,
			(void*) bfnPagina);
	if (paginaAActualizar != NULL) //Si ya existe actualizar
			{
		paginaAActualizar->timeStamp = getCurrentTime();

		guardarPaginaEnMP(paginaAActualizar->nroPagina, keyUint16, value, paginaAActualizar->timeStamp);
	pthread_mutex_unlock(&m_pagina);
	} else {
		pthread_mutex_unlock(&m_pagina);
		tipoNroPagina nroMarcoLibre;

		//posicionMarcoLibreBitarray = insertarEnMemoriaPrincipal(keyUint16, value, timestamp);
		nroMarcoLibre = setearMarcoLibreEnBitarray();

		if ((int) nroMarcoLibre == NO_ENCONTRO_MARCO_LIBRE) {

			nroMarcoLibre = algoritmoLRU(segmentoTabla, keyString, value,
					timestamp);

			if ((int) nroMarcoLibre == NO_ENCONTRO_MARCO_LIBRE) {
				if (connection != NULL) {
					runFunction(connection->socket, "memoriaPrincipalFull", 0);
					sem_wait (&esperarJournal);
				}
				JOURNAL_MEMORIA (NULL);
				pthread_mutex_lock(&m_segmento);
				nodoSegmento_t* segmentoTabla = buscarSegmentoPorNombreDeTabla(
						nombreTabla);
				if (segmentoTabla == NULL) {
					segmentoTabla = crearTablaSegmento(nombreTabla);
				}
				pthread_mutex_unlock(&m_segmento);

				//TODO creo de nuevo el segmento de la tabla ya que lo elimine en el journal anterior
				//posicionMarcoLibreBitarray = insertarEnMemoriaPrincipal(keyUint16, value, timestamp);
				nroMarcoLibre = setearMarcoLibreEnBitarray();
				pthread_mutex_lock(&m_pagina);
				guardarPaginaEnMP(nroMarcoLibre, keyUint16, value, timestamp);
				paginaAActualizar = crearNodoPagina(
						segmentoTabla->tablaDePaginas, true, -1, keyUint16,
						value, nroMarcoLibre); // paso parametro flag en true ya que inserto.
				pthread_mutex_unlock(&m_pagina);
			} else {
				pthread_mutex_lock(&m_pagina);
				paginaAActualizar = crearNodoPagina(
						segmentoTabla->tablaDePaginas, true, -1, keyUint16,
						value, nroMarcoLibre); // paso parametro flag en true ya que inserto.
				pthread_mutex_unlock(&m_pagina);
			}
		} else {

			pthread_mutex_lock(&m_pagina);
			guardarPaginaEnMP(nroMarcoLibre, keyUint16, value, timestamp);
			paginaAActualizar = crearNodoPagina(segmentoTabla->tablaDePaginas,
					true, -1, keyUint16, value, nroMarcoLibre); // paso parametro flag en true ya que inserto.
			pthread_mutex_unlock(&m_pagina);
		}

//		for (tipoNroPagina j = 0; j < list_size(segmentoTabla->tablaDePaginas); j++)
//		{
//			nodoPagina_t* unNodoPagina = list_get(segmentoTabla->tablaDePaginas,j);
//			printf("Segmento: %s. Nro de pagina: %u\n", segmentoTabla->nombreDeLaTabla, unNodoPagina->nroPagina);
//		}
	}
	log_info(logger, "Pasando Error3");
	return paginaAActualizar;
}

void guardarPaginaEnMP(tipoNroPagina nroMarco, uint16_t keyUint16, char* value,
		double timestamp) {
	char* tmpStringValue = malloc(tamanioValue + 1);
	memset(tmpStringValue, '\0', tamanioValue + 1);
	memcpy(tmpStringValue, value, strlen(value));
	tmpStringValue[strlen(value)] = '\0';

	memcpy(pMemoriaPrincipal + (nroMarco * tamanioDeMarco), &keyUint16,
			sizeof(uint16_t)); // guardo Key en MP
	memcpy(pMemoriaPrincipal + (nroMarco * tamanioDeMarco) + sizeof(uint16_t),
			&timestamp, sizeof(double)); //
	memcpy(
			pMemoriaPrincipal + (nroMarco * tamanioDeMarco) + sizeof(uint16_t)
					+ sizeof(double), tmpStringValue, tamanioValue); // guardo valor en MP

	free(tmpStringValue);
}

tipoNroPagina insertarEnMemoriaPrincipal(uint16_t keyUint16, char* value,
		double timestamp) {
	tipoNroPagina posicionMarcoLibreBitarray = setearMarcoLibreEnBitarray();

	if ((int) posicionMarcoLibreBitarray == -1) // si no hay marcos libres retorno error.
		return NO_ENCONTRO_MARCO_LIBRE;

	//guardarPaginaEnMP(posicionMarcoLibreBitarray, keyUint16, value, timestamp);

	return posicionMarcoLibreBitarray;
}

tipoNroPagina setearMarcoLibreEnBitarray() {
	pthread_mutex_lock (&m_bitarray);
	tipoNroPagina i;
	for (i = 0; i < cantidadMarcos; i++) {
		if (bitarray_test_bit(bitarrayMarcos, i) == false) {
			bitarray_set_bit(bitarrayMarcos, i);
			pthread_mutex_unlock(&m_bitarray);
			return i;
		}
	}
	pthread_mutex_unlock(&m_bitarray);
	return NO_ENCONTRO_MARCO_LIBRE;
}

tipoNroPagina buscarMarcoLibreEnBitarray(uint16_t keyUint16, char* value) {
	tipoNroPagina noEncontroMarcoLibre = NO_ENCONTRO_MARCO_LIBRE;

	tipoNroPagina i;
	for (i = 0; i < cantidadMarcos; i++) {
		if (bitarray_test_bit(bitarrayMarcos, i) == false) {
			return i;
		}
	}

	return NO_ENCONTRO_MARCO_LIBRE;
}

tipoNroPagina algoritmoLRU(nodoSegmento_t* segmentoTabla, char* keyString,
		char* value, double timestamp) {
	tipoNroPagina encontroMarcoLibre = NO_ENCONTRO_MARCO_LIBRE;
	double paginaMenosUsada = 99999999999999;

	nodoSegmento_t* tablaConPaginaLibreParaReemplazar;
	nodoPagina_t* paginaLibreAReemplazar = NULL;
	int indexPaginaAEliminar;

	// recorrer tablaDeSegmentos
	int tamanioListaDeSegmentos = list_size(tablaDeSegmentos);
	int contSegm;
	for (contSegm = 0; contSegm < tamanioListaDeSegmentos; contSegm++) {
		nodoSegmento_t* tablaActual;

		tablaActual = list_get(tablaDeSegmentos, contSegm);
		printf("Tabla Actual: %s\n", tablaActual->nombreDeLaTabla);

		// recorrer paginas con flag en false (registros no actualizados)
		int tamanioListaDePaginas = list_size(tablaActual->tablaDePaginas);
		printf(" -- Tamanio de la lista de paginas : %d \n",
				tamanioListaDePaginas);
		int contPag;
		for (contPag = 0; contPag < tamanioListaDePaginas; contPag++) {
			nodoPagina_t* paginaActual;

			paginaActual = list_get(tablaActual->tablaDePaginas, contPag);
			//printf(" -- Pagina Actual: nro de pagina: %u - flag: %d - timestamp: %f \n",paginaActual->nroPagina, paginaActual->flags, paginaActual->timeStamp);

			if (paginaActual->flags == false) {
				if (paginaActual->timeStamp < paginaMenosUsada) // si la pagina actual es menos usada que la guardada
						{
					log_info(logger, "Inicio del algoritmo LRU.");
					//printf("Pagina Candidata a reemplazar\n");
					paginaMenosUsada = paginaActual->timeStamp;
					encontroMarcoLibre = paginaActual->nroPagina;

					paginaLibreAReemplazar = paginaActual;
					tablaConPaginaLibreParaReemplazar = tablaActual;
					indexPaginaAEliminar = contPag;
				}
			}
		}
	}

	if (paginaLibreAReemplazar != NULL) {
		uint16_t keyUint16 = (uint16_t) atoi(keyString);

		pthread_mutex_lock(&m_pagina);
		guardarPaginaEnMP(paginaLibreAReemplazar->nroPagina, keyUint16, value,
				timestamp);


		list_remove_and_destroy_element(
				tablaConPaginaLibreParaReemplazar->tablaDePaginas,
				indexPaginaAEliminar, (void*) free);
		pthread_mutex_unlock(&m_pagina);
	}

	printf("no encontro marco libre al hacer LRU\n");
	return encontroMarcoLibre;
}

int JOURNAL_MEMORIA(socket_connection* connection) {
	pthread_mutex_lock (&m_journal);
	pthread_mutex_lock(&m_segmento);
	pthread_mutex_lock(&m_pagina);

	if (list_size(tablaDeSegmentos) == 0) {
		log_error(logger, "No Hay Datos Para Enviar");
	}

	int i;
	for (i = 0; i < list_size(tablaDeSegmentos); i++) {
		nodoSegmento_t* unNodoSegmento = list_get(tablaDeSegmentos, i);

		int j;
		for (j = 0; j < list_size(unNodoSegmento->tablaDePaginas); j++) {
			nodoPagina_t* unNodoPagina = list_get(
					unNodoSegmento->tablaDePaginas, j);

			if (unNodoPagina->flags == true) // Si el flag esta en Modificado
					{

				//envio el insert a LFS
				marco_t* marco = obtenerRegistroDesdeMemoria(unNodoPagina);

				//printf("Journal: timestamp antes del envio al LFS : _%s_\n", marco->timestamp);
				runFunction(socketLFS, "realizarInsertEnLFS", 4,
						unNodoSegmento->nombreDeLaTabla, marco->key,
						marco->value, marco->timestamp);
				sem_wait (&semInserts);

				free(marco->value);
				free(marco->key);
				free(marco->timestamp);
				free(marco);
			}
		}
	}

	if (list_size(tablaDeSegmentos) != 0) {
		list_clean_and_destroy_elements(tablaDeSegmentos,
				(void*) eliminarSegmentosYSusPaginas);
	}

	limpiarMarcos();

	if (connection != NULL) {
		runFunction(connection->socket, "finalizoJournal", 0);
	}
	pthread_mutex_unlock(&m_pagina);
	pthread_mutex_unlock(&m_segmento);
	pthread_mutex_unlock(&m_journal);
	return FN_SUCCESS;
}

void eliminarSegmentosYSusPaginas(nodoSegmento_t* nodoSegmento) {
	list_clean_and_destroy_elements(nodoSegmento->tablaDePaginas, (void*) free);
	list_destroy(nodoSegmento->tablaDePaginas);
	free(nodoSegmento->nombreDeLaTabla);
	free(nodoSegmento);
}

void limpiarMarcos() {
	pthread_mutex_lock (&m_bitarray);
	tipoNroPagina i;
	for (i = 0; i < cantidadMarcos; i++) {
		bitarray_clean_bit(bitarrayMarcos, i);
	}
	pthread_mutex_unlock(&m_bitarray);
}

nodoPagina_t* crearNodoPagina(t_list* tablaDePaginas, bool flagModificado,
		int posicionFrameLibre, uint16_t key, char* value,
		tipoNroPagina posicionMarcoLibreBitarray) {
	nodoPagina_t* nuevoNodoPagina;
	nuevoNodoPagina = malloc(sizeof(nodoPagina_t));
	nuevoNodoPagina->nroPagina = posicionMarcoLibreBitarray;
	nuevoNodoPagina->flags = flagModificado; //se coloca true si es un insert, false si select.
	nuevoNodoPagina->frame = posicionFrameLibre; // no se inicializa por ser un void *
	nuevoNodoPagina->timeStamp = getCurrentTime(); // obtener timestamp en ms

	list_add(tablaDePaginas, nuevoNodoPagina);

	//bitarrayImprimir1eros100();
	return nuevoNodoPagina;
}

void disconnect(socket_connection* connection) {
}

double getCurrentTime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long result = (((unsigned long long) tv.tv_sec) * 1000
			+ ((unsigned long long) tv.tv_usec) / 1000);
	double a = result;
	return a;
}

void DROP_MEMORIA(char* nombreTabla, socket_connection* connection) {
	bool _esTabla(nodoSegmento_t *segmento) {return string_equals_ignore_case(segmento->nombreDeLaTabla, nombreTabla);}

	pthread_mutex_lock (&m_segmento);
	nodoSegmento_t *tablaAEliminar = list_remove_by_condition(tablaDeSegmentos,
			(void*) _esTabla);
	if (tablaAEliminar == NULL) {
		log_error(logger, "No existe la tabla para eliminar: %s", nombreTabla);

		if (connection != NULL) {
			sleep(datosConfigMemoria->retardoFS);
			runFunction(socketLFS, "realizarDropEnLFS", 1, nombreTabla);
		}
		pthread_mutex_unlock(&m_segmento);
		return;
	}

	t_list* listaPaginas = tablaAEliminar->tablaDePaginas;
	int cantidadPaginas = list_size(listaPaginas);

	for (int indexPag = 0; indexPag < cantidadPaginas; indexPag++) {
		nodoPagina_t* nodoDeUnaPagina = list_get(listaPaginas, indexPag);
		if (nodoDeUnaPagina == NULL) // lista de paginas vacia
				{
			log_error(logger,
					"Error lista de paginas vacia: index pagina: %d - DROP %s",
					indexPag, nombreTabla);
			return;
		}
		pthread_mutex_lock (&m_bitarray);
		bitarray_clean_bit(bitarrayMarcos, nodoDeUnaPagina->nroPagina);
		pthread_mutex_unlock(&m_bitarray);
	}
	list_clean_and_destroy_elements(listaPaginas, (void*) free);
	free(tablaAEliminar->nombreDeLaTabla);
	list_destroy(tablaAEliminar->tablaDePaginas);
	free(tablaAEliminar);

	log_info(logger, "Se elimino la tabla: %s", nombreTabla);

	//bitarrayImprimir1eros100();

	if (connection != NULL) {
		sleep(datosConfigMemoria->retardoFS);
		runFunction(socketLFS, "realizarDropEnLFS", 1, nombreTabla);
	}
	pthread_mutex_unlock(&m_journal);
}

void imprimirBitarray() {
	//imprimir todos los marcos utilizados.
	uint16_t *keyUint16aux = malloc(sizeof(uint16_t));
	double *timestampactualaux = malloc(sizeof(double));
	printf("Tamaño del bitarray: %d\n", cantidadMarcos);
	for (tipoNroPagina j = 0; j < cantidadMarcos; j++) {
		if (bitarray_test_bit(bitarrayMarcos, j) == true) {
			memcpy(keyUint16aux, pMemoriaPrincipal + (j * tamanioDeMarco),
					sizeof(uint16_t));
			memcpy(timestampactualaux,
					pMemoriaPrincipal + (j * tamanioDeMarco) + sizeof(uint16_t),
					sizeof(double));
			printf("bitarray posicion: %u key %u timestamp: %f\n", j,
					*keyUint16aux, *timestampactualaux);
		}
	}
	free(keyUint16aux);
	free(timestampactualaux);
}

void imprimirMP() {
	//imprimir todos los marcos utilizados.
	printf(" ================= memoria contigua ================== \n");
	int j;
	for (j = 0; j < datosConfigMemoria->tamanioMemoria / tamanioDeMarco; j++) {
		uint16_t key;
		double timestampactual;
		char* tmpStringValue = malloc(tamanioValue + 1);
		memset(tmpStringValue, '\0', tamanioValue + 1);

		memcpy(&key, pMemoriaPrincipal + (j * tamanioDeMarco),
				sizeof(uint16_t)); // guardo Key
		memcpy(&timestampactual,
				pMemoriaPrincipal + (j * tamanioDeMarco) + sizeof(uint16_t),
				sizeof(double)); // guardo timestamp
		memcpy(tmpStringValue,
				pMemoriaPrincipal + (j * tamanioDeMarco) + sizeof(uint16_t)
						+ sizeof(double), tamanioValue); // guardo valor

		printf("     valor: %s \n", tmpStringValue);
		printf("       key: %u \n", key);
		printf(" timestamp: %f \n", timestampactual);

		free(tmpStringValue);
	}
}

void liberarDatosMemoria(t_config_Memoria* datosConfigMemoria) {

	free(datosConfigMemoria->ipFileSystem);
	string_iterate_lines(datosConfigMemoria->ipVectorMemorias, (void*) free);
	free(datosConfigMemoria->ipVectorMemorias);
	string_iterate_lines(datosConfigMemoria->puertosSeeds, (void*) free);
	free(datosConfigMemoria->puertosSeeds);
	free(datosConfigMemoria->memoryNumber);
	free(datosConfigMemoria);
}

void realizarGossip() {

	int cantidadSeeds = retornarCantidadDeElementos(
			datosConfigMemoria->ipVectorMemorias);

	if (cantidadSeeds == 0) {
		log_info(logger, "No Hay Memorias A La Cual Conectar");
		return;
	}

	for (int i = 0; i < cantidadSeeds; i++) {

		char * ip = datosConfigMemoria->ipVectorMemorias[i];
		int puerto = atoi(datosConfigMemoria->puertosSeeds[i]);

		bool tienenElMismoNumeroPuerto(nodoGossiping_t* self) {
			return self->puerto == puerto;
		}

		nodoGossiping_t* memoriaEncontrada = list_find(listaDeGossiping,
				(void*) tienenElMismoNumeroPuerto);

		if (memoriaEncontrada != NULL) {
			log_trace(logger,
					"Se Verificara Que El Memory Number: %s Sigue Activo",
					memoriaEncontrada->memoryNumber);

			if (!runFunction(memoriaEncontrada->socket, "pasarInfoGossip", 1,
					"Memoria")) {

				nodoGossiping_t* memoriaDesconectada = list_remove_by_condition(
						listaDeGossiping, (void*) tienenElMismoNumeroPuerto);

				log_info(logger,
						"El Memory Number: %s Del Socket: %d Se Elimino Del Gossip",
						memoriaDesconectada->memoryNumber,
						memoriaDesconectada->socket);
				free(memoriaDesconectada->memoryNumber);
				free(memoriaDesconectada->ip);
				free(memoriaDesconectada);
				sem_post (&esperaGossip);
			}
			sem_wait (&esperaGossip);
		}

		else {
			int socketConexion = connectServer(ip, puerto, llamadaFunciones,
					&disconnect, NULL);

			if (socketConexion == -1) {
				log_error(logger, "no se pudo conectar al ip %s  puerto %d", ip,
						puerto);
			} else {

				nodoGossiping_t *memoriaNueva = malloc(sizeof(nodoGossiping_t));
				memoriaNueva->ip = ip;
				memoriaNueva->puerto = puerto;
				memoriaNueva->socket = socketConexion;
				memoriaNueva->memoryNumber = NULL;

				pthread_mutex_lock(&m_agregarMemoria);
				list_add(listaDeGossiping,memoriaNueva);
				pthread_mutex_unlock(&m_agregarMemoria);

				runFunction(socketConexion, "pasarInfoGossip", 1,
						"Memoria");
				sem_wait (&esperaGossip);
			}
		}
	}
}

int retornarCantidadDeElementos(char** listaBloques) {
	int cantidadElementos = 0;
	while (listaBloques[cantidadElementos] != NULL) {
		cantidadElementos++;
	}
	return cantidadElementos;
}

void journalAPedidoDeKernel(socket_connection* connection) {

	pthread_t hiloJournal;
	pthread_create(&hiloJournal, NULL, (void*) JOURNAL_MEMORIA, NULL);
	pthread_detach(hiloJournal);
	runFunction(connection->socket, "finalizoJournal", 0);
}

void forzarJournalParaInsert(socket_connection* connection) {
	log_debug(logger, "forzarJournalParaInsert");
	sem_post (&esperarJournal);
}

void eliminarMemoriasConocidas(nodoGossiping_t* elemento) {
	free(elemento->memoryNumber);
	free(elemento);
}
