#include "consolaLissandra.h"
#include "libLissandra.h"

void consolaLissandra() {

	char* palabraValue = NULL;
	char* linea = NULL;
	char espaciosEnBlanco[4] = " \n\t";
	const char comillas = '"';
	int continuar = 1;

	char* primeraPalabra = NULL;
	char* segundaPalabra = NULL;
	char* terceraPalabra = NULL;
	char* cuartaPalabra = NULL;
	char* quintaPalabra = NULL;
	t_list* sentenciasEscritas = list_create();

	do {
		log_info(logger, "Bienvenido A La Consola, Ingrese Una Sentencia");
		linea = readline(">");

		string_to_upper(linea);
		primeraPalabra = strtok(linea, espaciosEnBlanco);

		palabraReservada = transformarStringToEnum(primeraPalabra);

		if (palabraReservada == INSERT) {
			segundaPalabra = strtok(NULL, espaciosEnBlanco);
			terceraPalabra = strtok(NULL, espaciosEnBlanco);
			cuartaPalabra = strtok(NULL, &comillas);
			quintaPalabra = strtok(NULL, &comillas);
			if(quintaPalabra != NULL){
			palabraValue = string_new();
			}
		}

		else{
			segundaPalabra = strtok(NULL, espaciosEnBlanco);
			terceraPalabra = strtok(NULL, espaciosEnBlanco);
			cuartaPalabra = strtok(NULL, espaciosEnBlanco);
			quintaPalabra = strtok(NULL, espaciosEnBlanco);
		}

		switch (palabraReservada) {

		case SELECT:

			infoSelect = malloc(sizeof(datos_Select));
			infoSelect->nombreTabla = segundaPalabra;
			infoSelect->key = terceraPalabra;
			seleccionarYEncontrarUltimaKeyActualizada(NULL,infoSelect);
			free(infoSelect);
			break;

		case INSERT:
			//Reservo Y Almaceno Los Datos Ingresados En La Estructura Para Su Uso
			infoInsert = malloc(sizeof(datos_Insert));
			infoInsert->nombreTabla = string_duplicate(segundaPalabra);
			infoInsert->key = string_duplicate(terceraPalabra);
			infoInsert->value = string_duplicate(cuartaPalabra);

			if (quintaPalabra != NULL) {
				string_append(&palabraValue, quintaPalabra);
				string_trim_left(&palabraValue);
				infoInsert->timestamp = palabraValue;
				list_add(sentenciasEscritas, palabraValue);
			}

			else {
				infoInsert->timestamp = NULL;
			}

			insertarValoresDentroDeLaMemTable(NULL, infoInsert);
			if (infoInsert != NULL) {
				free(infoInsert->nombreTabla);
				free(infoInsert);
			}
			break;

		case CREATE:
			//Reservo Bloque De Memoria Para Guardar Los Datos Pasados
			infoCreacion = malloc(sizeof(datos_Create));
			infoCreacion->nombreTabla = segundaPalabra;
			infoCreacion->tipoConsistencia = terceraPalabra;
			infoCreacion->numeroDeParticiones = cuartaPalabra;
			infoCreacion->tiempoDeCompactacion = quintaPalabra;
			crear_y_almacenar_datos(NULL,infoCreacion);
			free(infoCreacion);
			break;

		case DESCRIBE:

			if (segundaPalabra == NULL) {
				char* valor = recorridoDeTodasLasTablasPersistidas();
				free(valor);
			}
			else {
				recorridoDeUnaTabla(NULL, segundaPalabra);
			}
			break;

		case DROP:

			destruirEnCascadaElDirectorio(NULL, segundaPalabra);
			break;

		case SALIR:
			continuar = strcmp(linea, "SALIR");
			if (continuar)
				printf("comando no reconocido\n");
			break;
		default:
			break;
		}

		//Agrego A Una Lista Todas Las Lineas Ingresadas Utilizando La Consola
		//Cuando Dejo De Utilizarla Itero La Misma Liberandola
		list_add(sentenciasEscritas, linea);

	} while (continuar);

    flagSalida = 0;
    list_clean_and_destroy_elements(sentenciasEscritas, (void*) free);
    list_destroy(sentenciasEscritas);
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

	else if (string_contains(palReservada, "SALIR")) {
		palabraTransformada = SALIR;
	}

	return palabraTransformada;
}

//Funcion La Cual Primero Crea El Directorio De La Tabla (1 - Si Ya Existe, -1 - Si No Fue Creado)
void crear_y_almacenar_datos(socket_connection* connection, datos_Create* infoCreacion) {

	char* meta = "Metadata";
	struct stat *buf = malloc(sizeof(struct stat));

	char* path = retornarDireccionPrincipalConTabla(infoCreacion->nombreTabla);

	if (stat(path, buf) != -1) {

		log_error(logger, "El Directorio %s Ya Existe",
				infoCreacion->nombreTabla);
		free(buf);
		free(path);

		if (connection != NULL) {
			runFunction(connection->socket, "respuestaSobreCreate", 2, "CREATE",
					"0");
		}

		return;
	}

	if (!hayBloquesParaReservar(infoCreacion->numeroDeParticiones)) {
		log_error(logger, "No Hay Espacio Suficiente Para Crear La %s",
				infoCreacion->nombreTabla);
		free(buf);
		free(path);
		return;
	}

		free(buf);
		mkdir(path, 0700);

		log_info(logger, "Se Creo La Carpeta %s", infoCreacion->nombreTabla);

		string_append_with_format(&path, "/%s", meta);
		FILE* fileDirectory = fopen(path, "w+");

		t_config* confStandardMetadata = config_create(path);

			//Aca Se Setean Los Valores Del Metadata
			log_info(logger, "El Archivo %s Fue Creado", meta);
			log_trace(logger, "Se Setearan Los Siguientes Valores: %s, %s, %s",
					infoCreacion->tipoConsistencia,
					infoCreacion->numeroDeParticiones,
					infoCreacion->tiempoDeCompactacion);
			fprintf(fileDirectory, "CONSISTENCY=%s",
					infoCreacion->tipoConsistencia);
			fprintf(fileDirectory, "\n");
			fprintf(fileDirectory, "PARTITIONS=%s",
					infoCreacion->numeroDeParticiones);
			fprintf(fileDirectory, "\n");
			fprintf(fileDirectory, "COMPACTION_TIME=%s",
					infoCreacion->tiempoDeCompactacion);
			log_info(logger, "Se Crearon Los Datos A Ingresar");

			fclose(fileDirectory);
			config_destroy(confStandardMetadata);

		//Funcion Para Crear Las Particiones De Una Tabla En Particular
		crearParticiones(path, infoCreacion->nombreTabla);
		free(path);

		pthread_mutex_t m_tabla;
		pthread_t hiloParaTabla;
		pthread_create(&hiloParaTabla, NULL, (void*) controlEjecucionCompactacion, string_duplicate(infoCreacion->nombreTabla));

		t_createdTables* infoNodoTabla = malloc(sizeof(t_createdTables));
		char* duplicadoTabla = string_duplicate(infoCreacion->nombreTabla);
		infoNodoTabla->nombreTabla = duplicadoTabla;
		infoNodoTabla->contadorDumpeos = 1;
		infoNodoTabla->cantidadCompactados = 1;
		pthread_mutex_init(&m_tabla, NULL);
		infoNodoTabla->mutex_tabla = m_tabla;
		infoNodoTabla->hiloTabla = hiloParaTabla;
		infoNodoTabla->flagSalida = 1;
		pthread_mutex_lock(&m_insercion);
		list_add(listadoTablas, infoNodoTabla);
		pthread_mutex_unlock(&m_insercion);

		log_info(logger, "Se Agrego %s A La Lista", infoCreacion->nombreTabla);
		sem_post(&esperaTabla);

		if(connection != NULL){
			runFunction(connection->socket, "respuestaSobreCreate", 2, "CREATE", "1");
			log_debug(logger, "Envio Respuesta Create");
		}
	}

//Funcion Para Recorrer Todas Las Ramas Del Arbol Tables
char* recorridoDeTodasLasTablasPersistidas() {

	char* archivoMetadata = "/Metadata";
	//log_debug(logger, "Cantidad %d", list_size(listadoTablas));
	int cantidad = list_size(listadoTablas);

	if (cantidad == 0) {

		log_error(logger, "No Se Encontro Ningun Directorio En La Lista");
		return NULL;
	}

	int index = 0;
	t_createdTables* elementoTabla = list_get(listadoTablas, index);
	char* sentenciaTablas = string_new();

	while (elementoTabla != NULL) {

		char* nombreTabla = elementoTabla->nombreTabla;
		char* path = retornarDireccionPrincipalConTabla(nombreTabla);
		string_append(&path, archivoMetadata);

		mostrarInfoTabla(path, nombreTabla);
		sentenciaTablas = enviarTablasCreadasEnUno(path ,sentenciaTablas ,nombreTabla);

		index++;
		elementoTabla = list_get(listadoTablas, index);
		free(path);
	}
	return sentenciaTablas;
}

char* enviarTablasCreadasEnUno(char* path , char* sentenciaTablas , char* nombreTabla){

	int cantidadTablas = list_size(listadoTablas);

	t_config* configuracionAEnviar = config_create(path);

	char* sentenciaConsistencia = string_new();
	string_append(&sentenciaConsistencia, config_get_string_value(configuracionAEnviar,
			"CONSISTENCY"));
	int sentenciaParticiones = config_get_int_value(configuracionAEnviar,
			"PARTITIONS");
	int sentenciaCompactacion = config_get_int_value(configuracionAEnviar,
			"COMPACTION_TIME");

	config_destroy(configuracionAEnviar);

	if(cantidadTablas == 1 || string_is_empty(sentenciaTablas)){
		string_append_with_format(&sentenciaTablas, "%s %s %d %d", nombreTabla, sentenciaConsistencia, sentenciaParticiones, sentenciaCompactacion);
	}

	else { string_append_with_format(&sentenciaTablas, ",%s %s %d %d", nombreTabla, sentenciaConsistencia, sentenciaParticiones, sentenciaCompactacion);}

	free(sentenciaConsistencia);
	return sentenciaTablas;
}

//Funcion Para Recorrer La Rama De Arbol De Una Tabla
void recorridoDeUnaTabla(socket_connection* connection, char* tablaParametro) {

	char* archivoMetadata = "/Metadata";
	char* path = retornarDireccionPrincipalConTabla(tablaParametro);
	
	if(!verificarTablaEnFS(path)){
		if(connection != NULL){
			runFunction(connection->socket, "respuestaSobreDescribe", 2, "DESCRIBE", "0");
		}
		free(path);
		return;
	}
	string_append(&path, archivoMetadata);
	mostrarInfoTabla(path, tablaParametro);
	if(connection != NULL){
	enviarTablaEspecificada(connection, path, tablaParametro);
	}
	free(path);
}

void enviarTablaEspecificada(socket_connection* connection, char* path, char* nombreTabla){

	t_config* configuracionAEnviar = config_create(path);

	char* sentenciaConsistencia = config_get_string_value(configuracionAEnviar,
			"CONSISTENCY");
	int sentenciaParticiones = config_get_int_value(configuracionAEnviar,
			"PARTITIONS");
	int sentenciaCompactacion = config_get_int_value(configuracionAEnviar,
			"COMPACTION_TIME");

	char* stringParticion = string_itoa(sentenciaParticiones);
	char* stringCompactacion = string_itoa(sentenciaCompactacion);
	runFunction(connection->socket, "pasarInfoDescribe", 5, "1",nombreTabla,
			sentenciaConsistencia, stringParticion, stringCompactacion);
	config_destroy(configuracionAEnviar);
	free(stringParticion);
	free(stringCompactacion);
}

//Esta Funcion A Partir De Su Directorio, Busca Primero Los .bin a Eliminar, Despues El Metadata
//Y Por Ultimo Si El Directorio Esta Vacio Se Elimina, (Faltara Mas Adelante Agregar Para Borrar Los Temporales)
void destruirEnCascadaElDirectorio(socket_connection* connection, char* tablaADestruir) {

	int sentenciaParticiones = 0;
	int retorno;
	struct dirent *direntp = malloc(sizeof(struct dirent));

	char* path = retornarDireccionPrincipalConTabla(tablaADestruir);
	char* copiaPath = retornarDireccionPrincipalConTabla(tablaADestruir);

	if(!verificarTablaEnFS(path)){
		if(connection != NULL){
		runFunction(connection->socket, "respuestaSobreDrop", 2, "DROP", "0");
		}
		free(path);
		free(copiaPath);
		free(direntp);
		return;
	}
	DIR *dirp = opendir(path);

	t_createdTables* tablaBuscada = retornarElementoTabla(tablaADestruir);
	tablaBuscada->flagSalida = 0;
	pthread_join(tablaBuscada->hiloTabla, NULL);

	retornarCantidadParticionTabla(tablaADestruir, &sentenciaParticiones);

	for (int tmpIndex = 1; tmpIndex < (tablaBuscada->contadorDumpeos);
			tmpIndex++) {
		char letraExtension = retornarUltimoCaracter(tablaBuscada->nombreTabla);
		char* extensionArchivoDump = retornarDireccionPrincipalConTabla(
				tablaBuscada->nombreTabla);
		string_append_with_format(&extensionArchivoDump, "/%c%d.tmp",
				letraExtension, tmpIndex);
		eliminarContenidoAlmacenado(extensionArchivoDump, tablaBuscada->mutex_tabla);
	}

	for (int index = 1; index < (sentenciaParticiones + 1); index++) {
		char* extension = retornarDireccionPrincipalConTabla(tablaADestruir);
		string_append_with_format(&extension, "/%d.bin", index);
		eliminarContenidoAlmacenado(extension, tablaBuscada->mutex_tabla);
	}

	char* archivoMetadata = "Metadata";
	string_append_with_format(&path, "/%s", archivoMetadata);

	retorno = remove(path);

	if (retorno == 0) {
		log_info(logger, "Se Deleteo El %s Satisfactoriamente",
				archivoMetadata);
	}

	else {
		log_error(logger, "No Se Pudo Deletear El %s", archivoMetadata);
		return;
	}

	int verificarListaEnMemoria(t_memTable_LFS* tablaEnMemoria){
		return string_equals_ignore_case(tablaEnMemoria->nombreTabla, tablaADestruir);
	}

	t_memTable_LFS* tablaAEncontrar = list_find(listaDeMemTable, (void*) verificarListaEnMemoria);

	if (tablaAEncontrar != NULL) {
		list_clean_and_destroy_elements(listaDeMemTable, (void*) liberarDatos);
		log_trace(logger, "La %s Fue Removida De La MemTable", tablaADestruir);
	}

	pthread_mutex_destroy(&tablaBuscada->mutex_tabla);

	bool coincideElNombre(t_createdTables* self){
		return string_contains(self->nombreTabla, tablaADestruir);
	}

	t_createdTables* elementoARemover = list_remove_by_condition(listadoTablas, (void*) coincideElNombre);
	free(elementoARemover->nombreTabla);
	free(elementoARemover);

	int resultado = rmdir(copiaPath);

	if (resultado != -1) {
		log_info(logger, "Se Elimino El Directorio: %s Satisfactoriamente",
				tablaADestruir);
	}

	free(copiaPath);
	free(path);
	free(direntp);
	free(dirp);

	if(connection != NULL){
		runFunction(connection->socket, "respuestaSobreDrop", 2, "DROP", "1");
	}
}

void eliminarContenidoAlmacenado(char* path, pthread_mutex_t hiloMutex) {

	liberarBloquesDeParticion(path);
	pthread_mutex_lock(&hiloMutex);
	bool retorno = remove(path);
	pthread_mutex_unlock(&hiloMutex);

	if (retorno) {
		log_error(logger, "No Se Pudo Deletear El Archivo");
		free(path);
		return;

	}
	log_info(logger, "Se Deleteo El Archivo Satisfactoriamente");
	free(path);
}

/*
  Funcion En La Cual Ingresa Los Registros Pasados Del Insert De La Consola Al Nodo Tabla En Particular
  Guardandolo Al Final De Su Propia Lista. Determinacion Del TimeStamp Entre Dos Sucesos
  Si El Ultimo Parametro De La Estructura Es Vacio Se Determina Su Valor, Sino Es El Pasado Por Parametro
*/
void insertarValoresDentroDeLaMemTable(socket_connection* connection, datos_Insert* infoInsert) {

	char* path = retornarDireccionPrincipalConTabla(infoInsert->nombreTabla);

	if (!(verificarTablaEnFS(path) && tamanioKeyMenorQueElMaximo(infoInsert->key) && valueMenorQueElMaximo(infoInsert->value))) {
		if (connection != NULL) {
			runFunction(connection->socket, "respuestaSobreInsert", 2, "INSERT" ,"0");
		}
//		if(infoInsert->timestamp != NULL) { free(infoInsert->timestamp); }
		free(infoInsert);
		return;
	}

	int verificarListaEnMemoria(t_memTable_LFS* tablaEnMemoria) {

		if(list_size(listaDeMemTable) == 0) { return 0; }

		return string_contains(tablaEnMemoria->nombreTabla,
				infoInsert->nombreTabla);
	}

	pthread_mutex_lock(&m_busqueda);
	t_memTable_LFS* busquedaTabla = NULL;

	busquedaTabla = list_find(listaDeMemTable, (void*) verificarListaEnMemoria);

	if(busquedaTabla == NULL){
		t_memTable_LFS* infoNodoMem = malloc(sizeof(t_memTable_LFS));
		infoNodoMem->nombreTabla = string_duplicate(infoInsert->nombreTabla);
		infoNodoMem->list_Table = list_create();
		list_add(listaDeMemTable, infoNodoMem);
		log_info(logger, "Se Creo %s En La MemTable", infoInsert->nombreTabla);
		pthread_mutex_unlock(&m_busqueda);
		insertarValoresEnTabla(infoInsert, infoNodoMem);
		if(connection != NULL){ runFunction(connection->socket, "respuestaSobreInsert", 2, "INSERT" ,"1"); }
		free(path);
		return;
	}
	pthread_mutex_unlock(&m_busqueda);

	insertarValoresEnTabla(infoInsert, busquedaTabla);
	free(path);
	if(connection != NULL){ runFunction(connection->socket, "respuestaSobreInsert", 2, "INSERT" ,"1"); }
}

void insertarValoresEnTabla(datos_Insert* infoInsert, t_memTable_LFS* elementoMem)
{

	char buffer[10];
	unsigned timestamp;

	if (infoInsert->timestamp == NULL) {
		timestamp = (unsigned) time(NULL);
		sprintf(buffer, "%u", timestamp);
		infoInsert->timestamp = string_duplicate(buffer);
	}

	data_list_Table* valorDelNodo = malloc(sizeof(data_list_Table));
	valorDelNodo->timeStamp = infoInsert->timestamp;
	valorDelNodo->key = infoInsert->key;
	valorDelNodo->value = infoInsert->value;
	log_trace(logger, "Se Ingresaran Al Nodo = %s", infoInsert->nombreTabla);
	log_trace(logger, "Los Siguientes Datos: %s;%s;%s", infoInsert->timestamp,
			infoInsert->key, infoInsert->value);
	pthread_mutex_lock(&m_inserts);
	list_add(elementoMem->list_Table, valorDelNodo);
	pthread_mutex_unlock(&m_inserts);
	log_info(logger, "Los Valores Fueron Ingresados Exitosamente");
}

void seleccionarYEncontrarUltimaKeyActualizada(socket_connection* connection, datos_Select* datosSeleccionados) {

	unsigned timeStampMaximo = 0;
	int cantidadParticiones = 0;
	char* lineaConElMaximoTS = NULL;

	char* path = retornarDireccionPrincipalConTabla(
			datosSeleccionados->nombreTabla);

	if (!verificarTablaEnFS(path)) {
		log_error(logger, "La Tabla %s No Existe", datosSeleccionados->nombreTabla);
		if (connection != NULL) {
			runFunction(connection->socket, "respuestaSobreSelect", 2, "SELECT", "-1");
		}
		free(path);
		return;
	}

	retornarCantidadParticionTabla(datosSeleccionados->nombreTabla,
			&cantidadParticiones);

	t_createdTables* tablaObjetivo = retornarElementoTabla(
			datosSeleccionados->nombreTabla);
	t_memTable_LFS* tablaMem = retornarElementoMemTable(
			datosSeleccionados->nombreTabla);

//------------------------------------------------------------------------------------------//
	int index = 0;
	data_list_Table* registroObjetivo = NULL;

	if (tablaMem != NULL)
	{
		registroObjetivo = list_get(tablaMem->list_Table,
				index);
	}

	pthread_mutex_lock(&m_busqueda);
	pthread_mutex_lock(&tablaObjetivo->mutex_tabla);
	while (registroObjetivo != NULL) {

		if(registroObjetivo->key == NULL) {

		}

		else {

		int keyValor = atoi(registroObjetivo->key);
		int keyBuscado = atoi(datosSeleccionados->key);
		int timestampEncontrado = atoi(registroObjetivo->timeStamp);

		if (keyValor == keyBuscado) {

			char* duplicado = string_duplicate(registroObjetivo->timeStamp);
			unsigned timestampObtenido = (unsigned) atoi(duplicado);

			if (timestampObtenido > timeStampMaximo) {

				timeStampMaximo = timestampObtenido;
				char* almacenarRegistro = string_new();
				string_append_with_format(&almacenarRegistro, "%s;%s;%s",
						registroObjetivo->timeStamp, registroObjetivo->key,
						registroObjetivo->value);
				if(lineaConElMaximoTS != NULL) { free(lineaConElMaximoTS); }
				lineaConElMaximoTS = string_duplicate(almacenarRegistro);
				free(almacenarRegistro);
			}
			free(duplicado);
		}
		}
		index++;
		registroObjetivo = list_get(tablaMem->list_Table, index);
	}
	pthread_mutex_unlock(&tablaObjetivo->mutex_tabla);
	pthread_mutex_unlock(&m_busqueda);

//---------------------------------------------------------------------------------------//
	for (int tmpIndex = 1; tmpIndex < (tablaObjetivo->contadorDumpeos);
			tmpIndex++) {

		char* extensionArchivoDump = retornarDireccionPrincipalConTabla(
				tablaObjetivo->nombreTabla);
		char letraExtension = retornarUltimoCaracter(
				tablaObjetivo->nombreTabla);

		string_append_with_format(&extensionArchivoDump, "/%c%d.tmp",
				letraExtension, tmpIndex);

		lineaConElMaximoTS = buscarLineaMaxima(extensionArchivoDump, datosSeleccionados->key, &timeStampMaximo,lineaConElMaximoTS, tablaObjetivo);
		free(extensionArchivoDump);
	}
//---------------------------------------------------------------------------------------//
	for (int tmpIndex = 1; tmpIndex < (tablaObjetivo->cantidadCompactados);
			tmpIndex++) {

		char* otroPath = retornarDireccionPrincipalConTabla(
				tablaObjetivo->nombreTabla);
		char letraExtension = retornarUltimoCaracter(
				tablaObjetivo->nombreTabla);
		string_append_with_format(&otroPath, "/%c%d.tmpc", letraExtension,
				tmpIndex);

		lineaConElMaximoTS = buscarLineaMaxima(otroPath, datosSeleccionados->key, &timeStampMaximo,lineaConElMaximoTS, tablaObjetivo);

		free(otroPath);
	}
//---------------------------------------------------------------------------------------//
	int keySeleccionada = atoi(datosSeleccionados->key);
	int particionObjetivo = calculoModuloParticion(cantidadParticiones,
			keySeleccionada);

	string_append_with_format(&path, "/%d.bin", particionObjetivo);

	lineaConElMaximoTS = buscarLineaMaxima(path, datosSeleccionados->key, &timeStampMaximo,lineaConElMaximoTS, tablaObjetivo);

	if (lineaConElMaximoTS == NULL) {
		log_error(logger, "No Se Encontro La Key: %s", datosSeleccionados->key);
		free(lineaConElMaximoTS);
		free(path);
		if(connection != NULL){
		runFunction(connection->socket, "valorEncontradoEnSelect",3, "0", datosSeleccionados->nombreTabla, datosSeleccionados->key);
		}
		return;
	}
	if(connection != NULL){
	runFunction(connection->socket, "valorEncontradoEnSelect", 3, "1" ,lineaConElMaximoTS, datosSeleccionados->nombreTabla);
	}
	log_trace(logger, "El Registro Mas Reciente Buscado En La %s Es %s",
			datosSeleccionados->nombreTabla, lineaConElMaximoTS);
	free(lineaConElMaximoTS);
	free(path);
}

char* buscarLineaMaxima(char* path, char* keyPasada, unsigned* maximoTimeStamp,char* lineaConElMaximoTS, t_createdTables* tablaObjetivo){

	char* lineaLeida = NULL;
	size_t buffersize = 0;

		pthread_mutex_lock(&tablaObjetivo->mutex_tabla);
		t_config* punteroArchTemporal = config_create(path);
		FILE* punteroArchivo = fopen(path, "r");
		pthread_mutex_unlock(&tablaObjetivo->mutex_tabla);

		if(punteroArchivo == NULL){ return lineaConElMaximoTS; }

		if(punteroArchTemporal == NULL) {
			log_error(logger, "No Existe El Archivo A Buscar La Key");
			return lineaConElMaximoTS;
		}

		fseek(punteroArchivo, 0, SEEK_END);
		int posicion = ftell(punteroArchivo);
		if(posicion == 0) {
			log_error(logger, "El Archivo No Esta Escrito");
			return lineaConElMaximoTS;
		}

		char** listaBloques = config_get_array_value(punteroArchTemporal, "BLOCKS");
		config_destroy(punteroArchTemporal);

		int indice = 0;
		int cantidadElementos = retornarCantidadDeBlocksEnParticion(listaBloques);
		char* palabraAlmacenada = NULL;

		while (indice < cantidadElementos) {

			char* direccionBloques = retornarDireccionBloques();
			string_append_with_format(&direccionBloques, "/%s.bin",
					listaBloques[indice]);

			FILE* ficheroParticion = fopen(direccionBloques, "r");

			if(ficheroParticion == NULL){ return lineaConElMaximoTS; }

			while (getline(&lineaLeida, &buffersize, ficheroParticion) != -1) {

				char* lineaPasada = string_duplicate(lineaLeida);

				if(string_is_empty(lineaPasada) || lineaPasada == NULL){
					return lineaConElMaximoTS;
				}

				if (palabraAlmacenada == NULL) {
					int largoCadena = string_length(lineaPasada);
					if (lineaPasada[largoCadena - 1] != '\n') {
						palabraAlmacenada = string_duplicate(lineaPasada);
						
						free(lineaPasada);
						break;
					}
				}

				if (palabraAlmacenada != NULL) {
					int largoCadena = string_length(lineaPasada);
					if (lineaPasada[largoCadena - 1] == '\n') {
						string_append(&palabraAlmacenada, lineaPasada);
						free(lineaPasada);
						lineaPasada = string_duplicate(palabraAlmacenada);
						
						free(palabraAlmacenada);
						palabraAlmacenada = NULL;
					}
				}
				
				char* timestampV = strtok(lineaPasada, ";");
				char* keyV = strtok(NULL, ";");
				char* valueV = strtok(NULL, ";");

				if(keyV == NULL && keyPasada == NULL){}

				else{
				int keyValor = atoi(keyV);
				int keyBuscado = atoi(keyPasada);

				if (keyValor == keyBuscado) {

					unsigned timestampObtenido = (unsigned) atoi(timestampV);

					if (timestampObtenido > *maximoTimeStamp) {
						string_append_with_format(&lineaPasada, ";%s;%s", keyV, valueV);
						char* lineaCopia = string_duplicate(lineaPasada);
						if(lineaConElMaximoTS != NULL) { free(lineaConElMaximoTS); }
						*maximoTimeStamp = timestampObtenido;
						lineaConElMaximoTS = lineaCopia;
					}
				}
				}
				free(lineaPasada);
			}
			fclose(ficheroParticion);
			free(direccionBloques);
			indice++;
		}

		string_iterate_lines(listaBloques, (void*) free);
		free(listaBloques);
		free(lineaLeida);
		return lineaConElMaximoTS;
}

void planificarDump(){
	log_trace(logger, "Se Creo El Hilo");
	unsigned inicioDeDump = (unsigned) time(NULL);
	int tiempoDumpMecanico = datosConfigLissandra->tiempoDump;
	int valorRetardo = datosConfigLissandra->retardo;
	int contadorDeDumpeos = 1;

	while(flagSalida){

		sleep(tiempoDumpMecanico/1000);

		sem_wait(&iNotify);
		if(inicioDeDump != datosConfigLissandra->tiempoDump){
		inicioDeDump = datosConfigLissandra->tiempoDump;
		}
		sem_post(&iNotify);

		log_info(logger, "Se Iniciara El Proceso Dump");
		iniciarProcesoDUMP();
	}

	log_trace(logger,"Se Sale Del Programa De Dumpeo");
}

bool esTiempoDeEjecutarse(unsigned inicioTiempo, unsigned nuevoTiempo, int tiempoACumplir) {
	return (nuevoTiempo - inicioTiempo) == tiempoACumplir;
}

bool hayBloquesParaReservar(char* numeroDeParticiones){

	int cantidadParticiones = atoi(numeroDeParticiones);
	int cantidadLibre = cantidadDeBloquesLibres();

	return cantidadParticiones <= cantidadLibre;
}

int cantidadDeBloquesLibres() {

	int acumuladorLibres = 0;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {

		if (bitarray_test_bit(bitarray, i) == 0) {
			acumuladorLibres++;
		}
	}
	return acumuladorLibres;
}

void controlEjecucionCompactacion(void* tabla) {

	sem_wait(&esperaTabla);
	char* nombreTabla = (char*) tabla;

	unsigned tiempoDeCreacion = (unsigned) time(NULL);
	int contadorDeCompactados = 1;
	int valorRetardo = datosConfigLissandra->retardo;

	bool coincideElNombreDeLaTablaCreada(t_createdTables* estructuraTabla) {
		return string_contains(estructuraTabla->nombreTabla, nombreTabla);
	}

	t_createdTables* estructuraPropia = list_find(listadoTablas,
			(void*) coincideElNombreDeLaTablaCreada);

	int tiempoACompactar = (retornarTiempoDeCompactacionARealizar(nombreTabla) / 1000);
	log_trace(logger, "Se Establecieron Los Datos Del Hilo De %s", nombreTabla);

	while (estructuraPropia->flagSalida) {

		sleep(tiempoACompactar);
		log_info(logger, "Se Iniciara El Proceso De Compactacion En La %s",
				nombreTabla);
		iniciarEnUnaTablaLaCOMPACTACION(nombreTabla);

	}
	log_trace(logger, "Se Finaliza El Hilo De Compactacion De La %s",
			nombreTabla);
	free(nombreTabla);
}

int retornarTiempoDeCompactacionARealizar(char* nombreTabla){
	char* path = retornarDireccionPrincipalConTabla(nombreTabla);
	string_append_with_format(&path, "/Metadata" );
	t_config* configTabla = config_create(path);
	int tiempoCompactacion = config_get_int_value(configTabla, "COMPACTION_TIME");
	config_destroy(configTabla);
	free(path);
	return tiempoCompactacion;
}
