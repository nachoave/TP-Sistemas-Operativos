#include "libLissandra.h"
#include "consolaLissandra.h"

//Funcion Sobre Archivo De Logs
void configure_logger() {

	char * nombrePrograma = "lissandra.log";
	char * nombreArchivo = "Lissandra";

	logger = log_create(nombrePrograma, nombreArchivo, false, LOG_LEVEL_TRACE);
	log_info(logger, "Se genero log del Lissandra");
}

t_config_Lissandra * read_and_log_config(char* path) {

	log_info(logger, "Voy a leer el archivo lissandra.config");

	t_config* archivo_Config = config_create(path);
	if (archivo_Config == NULL) {
		log_error(logger, "No existe archivo de configuracion");
		exit(1);
	}

	datosConfigLissandra = malloc(sizeof(t_config_Lissandra));

	int puertoFS = config_get_int_value(archivo_Config, "PUERTO_ESCUCHA");
	datosConfigLissandra->puertoEscucha = puertoFS;

	char* puntoMontajeFS = string_new();
	string_append(&puntoMontajeFS,
			config_get_string_value(archivo_Config, "PUNTO_MONTAJE"));
	datosConfigLissandra->ptoMontaje = puntoMontajeFS;

	datosConfigLissandra->retardo = config_get_int_value(archivo_Config,
			"RETARDO");
	datosConfigLissandra->tamanioValue = config_get_int_value(archivo_Config,
			"TAMAÑO_VALUE");
	datosConfigLissandra->tiempoDump = config_get_int_value(archivo_Config,
			"TIEMPO_DUMP");

	log_info(logger, "PUERTO_ESCUCHA: %d", datosConfigLissandra->puertoEscucha);
	log_info(logger, "PUNTO_MONTAJE: %s", datosConfigLissandra->ptoMontaje);
	log_info(logger, "RETARDO: %d", datosConfigLissandra->retardo);
	log_info(logger, "TAMAÑO_VALUE: %d", datosConfigLissandra->tamanioValue);
	log_info(logger, "TIEMPO_DUMP: %d", datosConfigLissandra->tiempoDump);

	log_info(logger, "Fin de lectura");
	config_destroy(archivo_Config);

	return datosConfigLissandra;
}

void close_logger() {
	log_info(logger, "Cierro log del Lissandra");
	log_destroy(logger);
}
//-------------------------------------------------------------------------------//

//Funciones De Creacion De Archivos Y Directorios
void crearPuntoDeMontajeYTablesLFS(){
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	mkdir(path, 0700);
	string_append_with_format(&path, "/Tables");
	mkdir(path, 0700);
	log_info(logger, "Se Creo El Punto De Montaje %s", datosConfigLissandra->ptoMontaje);
	free(path);
}

void establecerLaMetadataDeLFS() {

	struct stat *buf = malloc(sizeof(struct stat));
	char* meta = "Metadata";
	char* metabin = "Metadata.bin";
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&path, "/%s", meta);

	if (stat(path, buf) != -1) {

		log_warning(logger, "El Directorio %s Ya Existe", meta);
		free(buf);
		free(path);
		return;
	}

	mkdir(path, 0700);
	free(buf);
	log_info(logger, "Se Creo La Carpeta %s", meta);

	string_append_with_format(&path, "/%s", metabin);
	FILE* punteroMetadata = fopen(path, "w+");

	log_info(logger, "Se Creo El Archivo %s", metabin);

	fprintf(punteroMetadata, "BLOCK_SIZE=64\n");
	fprintf(punteroMetadata, "BLOCKS=4096\n");
	fprintf(punteroMetadata, "MAGIC_NUMBER=LISSANDRA\n");

	log_info(logger, "Se Escribieron Los Datos Del Metadata FS");
	fclose(punteroMetadata);
	free(path);
}

void crearBitMap() {

	char* meta = "Metadata";
	char* bloques = "Bloques";
	char* bitmap = "Bitmap.bin";
	char* path = string_new();
	int cantidadBloques = datosFileSystem->cantidadBloques / 8;

	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&path, "/%s", meta);

	verificarTablaEnFS(path);

	string_append_with_format(&path, "/%s", bitmap);

	int fdbitmap = open(path, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

	ftruncate(fdbitmap, cantidadBloques);

	char* bmap = mmap(0, cantidadBloques, PROT_WRITE | PROT_READ, MAP_SHARED, fdbitmap,
			0);

	if (bmap == MAP_FAILED) {
		log_error(logger, "Error al mapear a memoria: %s\n", strerror(errno));
		close(fdbitmap);
		free(path);
		free(bmap);
		return;
	}

	bitarray = bitarray_create_with_mode(bmap, cantidadBloques, MSB_FIRST);
	msync(bitarray, sizeof(bitarray), MS_SYNC);
	free(path);
}

void crearParticiones(char* path, char* nombreTabla) {

	t_config* archivoConfig = config_create(path);

	if (archivoConfig == NULL) {
		log_error(logger, "No Se Pudo Localizar");
		return;
	}

	int numeroParticiones = config_get_int_value(archivoConfig, "PARTITIONS");
	config_destroy(archivoConfig);

	for (int i = 1; i < (numeroParticiones + 1); i++) {
		char* pathAcotado = retornarDireccionPrincipalConTabla(nombreTabla);
		string_append_with_format(&pathAcotado, "/%d.bin", i);
		establecerBloqueYDatosAOcupar(pathAcotado);
		log_info(logger, "Se Creo La Particion: %d.bin", i);
		free(pathAcotado);
	}
}

void crearArchivosBinTotalesFS() {

	struct stat *buf = malloc(sizeof(struct stat));
	char* pathBloques = string_new();
	char* dirBloques = "Bloques";
	char* pathRecibido = retornarDireccionMetadata();

	string_append(&pathBloques, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&pathBloques, "/%s", dirBloques);

	if (stat(pathBloques, buf) != -1) {
		log_warning(logger, "El Directorio %s Ya Existe", dirBloques);
		setearMetadataBloques(pathRecibido);
		free(pathRecibido);
		free(pathBloques);
		free(buf);
		return;
	}

	free(buf);
	mkdir(pathBloques, 0700);

	setearMetadataBloques(pathRecibido);

	FILE* bloques[datosFileSystem->cantidadBloques];

	for (int index = 0; index < datosFileSystem->cantidadBloques; index++) {
		char* otroPath = string_new();
		string_append(&otroPath, datosConfigLissandra->ptoMontaje);
		string_append_with_format(&otroPath, "/%s", dirBloques);
		string_append_with_format(&otroPath, "/%d.bin", index);
		bloques[index] = fopen(otroPath, "w+");
		fclose(bloques[index]);
		free(otroPath);
	}

	free(pathRecibido);
	free(pathBloques);
}

void setearMetadataBloques(char* pathRecibido){

	t_config* configMeta = config_create(pathRecibido);

	int tamanioBloques = config_get_int_value(configMeta, "BLOCK_SIZE");
	int cantidadBloques = config_get_int_value(configMeta, "BLOCKS");
	char* magicNumber = config_get_string_value(configMeta, "MAGIC_NUMBER");

	datosFileSystem = malloc(sizeof(t_data_FS));
	datosFileSystem->tamanioBloques = (size_t) tamanioBloques;
	datosFileSystem->cantidadBloques = cantidadBloques;
	datosFileSystem->magicNumber = magicNumber;

	config_destroy(configMeta);
}
//-------------------------------------------------------------------------------//

//Funciones De Destruccion De Archivos Y Directorios
void destruirMontajeYTables(){
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&path, "/Tables");
	rmdir(path);
	free(path);
	rmdir(datosConfigLissandra->ptoMontaje);
	log_info(logger, "Se Elimino El Punto De Montaje %s", datosConfigLissandra->ptoMontaje);
}

void destruirCarpetaMetadata() {

	char* rutaBitmap = "Bitmap.bin";
	char* rutaMetadata = "Metadata";
	char* archivoMetadata = "Metadata.bin";
	char* path = string_new();
	char* ultimoPath = string_new();
	char* pathDirectorio = string_new();

	struct dirent *direntp = malloc(sizeof(struct dirent));

	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&path, "/%s", rutaMetadata);

	string_append(&pathDirectorio, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&pathDirectorio, "/%s", rutaMetadata);

	DIR *dirp = opendir(pathDirectorio);
	string_append_with_format(&path, "/%s", rutaBitmap);
	int retorno = remove(path);

	if (retorno != -1) {
		log_info(logger, "Se Elimino El Archivo %s", rutaBitmap);
	}

	char* otroPath = retornarDireccionMetadata();
	int segundoRetorno = remove(otroPath);

	if (segundoRetorno == -1) {
		log_error(logger, "No Se Pudo Eliminar El Archivo %s", archivoMetadata);
	}

	log_info(logger, "Se Elimino El Archivo %s", archivoMetadata);
	string_append(&ultimoPath, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&ultimoPath, "/%s", rutaMetadata);
	int resultado = rmdir(pathDirectorio);

	if (resultado != -1) {
		log_info(logger, "Se Elimino El Directorio: %s Satisfactoriamente",
				rutaMetadata);
	}

	free(path);
	free(otroPath);
	free(ultimoPath);
	free(pathDirectorio);
	closedir(dirp);
	free(direntp);
}

void destruirBloquesLFS() {

	char* rutaBloques = "Bloques";
	char* otroPath = string_new();

	struct dirent *direntp = malloc(sizeof(struct dirent));

	char* path = retornarDireccionMetadata();

	t_config* configMeta = config_create(path);
	int cantidadBloquesAEliminar = config_get_int_value(configMeta, "BLOCKS");
	config_destroy(configMeta);

	string_append(&otroPath, datosConfigLissandra->ptoMontaje);
	string_append_with_format(&otroPath, "/%s", rutaBloques);
	DIR *dirp = opendir(otroPath);

	FILE* bloques[cantidadBloquesAEliminar];

	for (int index = 0; index < cantidadBloquesAEliminar; index++) {
		char* extensionPath = string_new();
		string_append(&extensionPath, datosConfigLissandra->ptoMontaje);
		string_append_with_format(&extensionPath, "/%s", rutaBloques);
		string_append_with_format(&extensionPath, "/%d.bin", index);
		remove(extensionPath);
		free(extensionPath);
	}

	log_info(logger, "Se Eliminaron Todos Los Bloques Del Sistema");

	int resultado = rmdir(otroPath);

	if (resultado != -1) {
		log_info(logger, "Se Elimino El Directorio: %s Satisfactoriamente",
				rutaBloques);
	}

	free(dirp);
	free(direntp);
	free(path);
	free(otroPath);
}
//-------------------------------------------------------------------------------//

//Funciones De Retorno De Valores
char* retornarDireccionMetadata() {
	char* pathMetadata = string_new();
	string_append(&pathMetadata, datosConfigLissandra->ptoMontaje);
	string_append(&pathMetadata, "/Metadata");
	string_append(&pathMetadata, "/Metadata.bin");
	return pathMetadata;
}

char* retornarDireccionBloques(){
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append(&path, "/Bloques/");
	return path;
}

char* retornarDireccionPrincipalConTabla(char* nombreTabla){
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append(&path, "/Tables/");
	if (nombreTabla != NULL) {
		string_append(&path, nombreTabla);
		return path;
	}
	return path;
}

t_createdTables* retornarElementoTabla(char* nombreTabla){
	int index = 0;
	t_createdTables* elementoTabla = list_get(listadoTablas, index);

	while (!(strstr(elementoTabla->nombreTabla, nombreTabla) != NULL)) {
		index++;
		elementoTabla = list_get(listadoTablas, index);
	}
	return elementoTabla;
}

t_memTable_LFS* retornarElementoMemTable(char* nombreTabla){

	log_debug(logger,"                nombreTabla: %s\n", nombreTabla);

	int verificarListaEnMemoria(t_memTable_LFS* tablaEnMemoria){

//		for (int j = 0; j < list_size(tablaEnMemoria->list_Table); j++)
//		{
//			data_list_Table* unNodoPagina = list_get(tablaEnMemoria->list_Table,j);
//			printf("retornarElementoMemTable: timestamp _%s_\n", unNodoPagina->timeStamp);
//			printf("retornarElementoMemTable: key       _%s_\n", unNodoPagina->key);
//			printf("retornarElementoMemTable: value     _%s_\n", unNodoPagina->value);
//		}
		return string_equals_ignore_case(tablaEnMemoria->nombreTabla, nombreTabla);
	}

	t_memTable_LFS* elementoTabla = list_find(listaDeMemTable, (void*) verificarListaEnMemoria);

	return elementoTabla;
}

char retornarUltimoCaracter(char *nombreTabla) {
	int index = 0;
	int longCadena = string_length(nombreTabla);
	char c;
	char cadena[longCadena];
	strcpy(cadena, nombreTabla);

	while (index < longCadena) {

		if (index == (longCadena - 1)) {
			c = cadena[index];
		}
		index++;
	}
	return c;
}

void retornarCantidadParticionTabla(char* nombreTabla, int* cantidadParticiones) {
	char* path = string_new();
	char* dataMeta = "/Metadata";

	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append(&path, "/Tables/");
	string_append(&path, nombreTabla);
	string_append(&path, dataMeta);


	t_config* configMetadata = config_create(path);

	if(configMetadata == NULL) {
		log_error(logger, "No Existe La Metadata De %s", nombreTabla);

		free(path);
	}

	*cantidadParticiones = config_get_int_value(configMetadata, "PARTITIONS");
	config_destroy(configMetadata);
	free(path);
}

int retornarCantidadDeBlocksEnParticion(char** listaBloques){
	int cantidadElementos = 0;
	while(listaBloques[cantidadElementos] != NULL) { cantidadElementos++; }
	return cantidadElementos;
}
//-------------------------------------------------------------------------------//

//Funciones De Verificacion
int verificarTablasExistentes(){

	pthread_mutex_t m_tabla;
	resultados = NULL;
	char* path = string_new();
	string_append(&path, datosConfigLissandra->ptoMontaje);
	string_append(&path, "/Tables");

	int cantidadTablas = scandir(path, &resultados, NULL, alphasort);

	if (cantidadTablas <= 2) {

		log_info(logger, "No Existe Ninguna Tabla Por El Momento");
		liberarReferenciasDeTablas(cantidadTablas);
		free(path);
		return 0;
	}

	for (int i = 2; i < cantidadTablas; i++) {
		t_createdTables* infoNodoTabla = malloc(sizeof(t_createdTables));
		infoNodoTabla->nombreTabla = resultados[i]->d_name;
		infoNodoTabla->contadorDumpeos = 1;
		infoNodoTabla->cantidadCompactados = 1;
		pthread_mutex_init(&m_tabla ,NULL);
		infoNodoTabla->mutex_tabla = m_tabla;
		list_add(listadoTablas, infoNodoTabla);
	}

	free(path);
	return cantidadTablas;
}

int verificarTablaEnFS(char* verificacion){

	struct stat *buf = malloc(sizeof(struct stat));

	if (stat(verificacion, buf) == -1) {
		log_error(logger, "El Directorio %s No Existe", verificacion);
		free(buf);
		return 0;
	}

	free(buf);
	return 1;
}

bool valueMenorQueElMaximo(char* valueTamanio){

	int tamanioDelValue = string_length(valueTamanio) * sizeof(char);
	int tamanioValueMaximo = datosConfigLissandra->tamanioValue * sizeof(char);
	bool respuesta = tamanioDelValue <= tamanioValueMaximo;
	if(!respuesta){
		log_error(logger, "El Value = %s Supera El Tamanio Maximo = %d Bytes", valueTamanio, tamanioValueMaximo);
	}
	return respuesta;
}

bool tamanioKeyMenorQueElMaximo(char* keyTamanio) {

	uint16 valorMaximo = 0xFFFF;
	bool respuesta = atoi(keyTamanio) <= valorMaximo;
	if(!respuesta){
		log_error(logger, "La Key = %s Supera El Tamanio Maximo = %d", keyTamanio, valorMaximo);
	}
	return respuesta;
}
//-------------------------------------------------------------------------------//

//Funciones Utilizando BitArray
int establecerBloqueYDatosAOcupar(char* path) {

	int bloqueLibre = buscarBloqueLibre();

	if(bloqueLibre == -1){
		return -1;
	}

	FILE* punteroArchivo = fopen(path, "w+");
	char* tamanio = "SIZE=0\n";
	char* bloque = string_new();

	string_append_with_format(&bloque, "BLOCKS=[%d]", bloqueLibre);

	fwrite(tamanio, 1, strlen(tamanio), punteroArchivo);
	fwrite(bloque, 1, strlen(bloque), punteroArchivo);

	fclose(punteroArchivo);
	free(bloque);
	return 1;
}

void liberarBloquesDeParticion(char* path) {

	char* pathBloque = string_new();
	string_append(&pathBloque, datosConfigLissandra->ptoMontaje);
	string_append(&pathBloque, "/Bloques/");
	t_config* punteroBloques = config_create(path);

	char** listaBloques = config_get_array_value(punteroBloques, "BLOCKS");

	int cantidadElementos = 0;
	while (listaBloques[cantidadElementos] != NULL) { cantidadElementos++; }

	for (int i = 0; i < cantidadElementos; i++) {
		int posicionALiberar = atoi(listaBloques[i]);
		pthread_mutex_lock(&m_bitarray);
		bitarray_clean_bit(bitarray, posicionALiberar);
		pthread_mutex_unlock(&m_bitarray);

		char* pathDelBloque = string_new();
		string_append(&pathDelBloque, pathBloque);
		string_append_with_format(&pathDelBloque, "%d.bin", posicionALiberar);
		int retorno = remove(pathDelBloque);

		if (retorno != 0) {
			log_error(logger, "No Se Pudo Liberar El Bloque %d.bin", posicionALiberar);
		}

		log_info(logger, "Se Libero El Bloque %d.bin Satisfactoriamente",
							posicionALiberar);

		FILE* recreacionBloque = fopen(pathDelBloque, "a+");
		fclose(recreacionBloque);
		free(pathDelBloque);
	}
	free(pathBloque);
	config_destroy(punteroBloques);
	string_iterate_lines(listaBloques, (void*) free);
	free(listaBloques);
}

int buscarBloqueLibre() {
	int posicionLibre;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {
		pthread_mutex_lock(&m_bitarray);
		if (bitarray_test_bit(bitarray, i) == 0) {
			posicionLibre = i;
			bitarray_set_bit(bitarray, i);
			pthread_mutex_unlock(&m_bitarray);
			return posicionLibre;
		}
		pthread_mutex_unlock(&m_bitarray);
	}
	return -1;
}

void recrearBloqueVacio(char* path){
	remove(path);
	FILE* puntBloque = fopen(path, "w+");
	fclose(puntBloque);
}

void reservarArrayBloques(int almacenBloques[], int cantidadBloques){

	for(int i = 0; i < cantidadBloques; i++){
		almacenBloques[i] = buscarBloqueLibre();
	}
}

void recorrerTablasExistentesParaOcuparBloques() {
	list_iterate(listadoTablas, (void*) obtenerElementosPersistidos);
}

void obtenerElementosPersistidos(t_createdTables* self) {

	char* nombreTabla = self->nombreTabla;
	char* metadata = "Metadata";
	char* path = retornarDireccionPrincipalConTabla(nombreTabla);
	string_append_with_format(&path, "/%s", metadata);

	t_config* configMeta = config_create(path);

	int cantidadParticiones = config_get_int_value(configMeta, "PARTITIONS");

	config_destroy(configMeta);

	for (int i = 1; i < (cantidadParticiones + 1); i++) {

		char* otroPath = retornarDireccionPrincipalConTabla(nombreTabla);
		string_append_with_format(&otroPath, "/%d.bin", i);

		char** listaBloques;
		t_config* punteroBloques = config_create(otroPath);

		listaBloques = config_get_array_value(punteroBloques, "BLOCKS");

		int cantidadElementos = 0;
		while(listaBloques[cantidadElementos] != NULL) { cantidadElementos++; }

		for (int i = 0; i < cantidadElementos; i++) {
			int posicionAOcupar = atoi(listaBloques[i]);
			bitarray_set_bit(bitarray, posicionAOcupar);
		}

		config_destroy(punteroBloques);
		string_iterate_lines(listaBloques, (void*) free);
		free(listaBloques);
		free(otroPath);
	}
	free(path);
}
//-------------------------------------------------------------------------------//

//Funciones Sobre Registros
void insertarRegistroDentroDelBloque(char* contenidoRegistro, char* pathParticion) {

	char* contenidoDuplicado = string_duplicate(contenidoRegistro);
	char* pathBloque = string_new();
	string_append(&pathBloque, datosConfigLissandra->ptoMontaje);
	string_append(&pathBloque, "/Bloques/");
	char* pathMetadata = retornarDireccionMetadata();

	t_config* configMetadata = config_create(pathMetadata);
	int tamanioBloque = config_get_int_value(configMetadata, "BLOCK_SIZE");
	config_destroy(configMetadata);
	free(pathMetadata);

	t_config* punteroBloques = config_create(pathParticion);
	int tamanioArchivo = config_get_int_value(punteroBloques, "SIZE");
	char** listaBloques = config_get_array_value(punteroBloques, "BLOCKS");
	config_destroy(punteroBloques);

	char* bloques = string_new();
	char* path = string_new();
	string_append(&path, pathBloque);

	int cantidadElementos = retornarCantidadDeBlocksEnParticion(listaBloques);

	for(int indice = 0; indice < cantidadElementos; indice++){

	char* pathDelBloque = string_new();
	string_append(&pathDelBloque, pathBloque);
	string_append_with_format(&pathDelBloque, "%s.bin", listaBloques[indice]);

	FILE* punteroArchivo = fopen(pathDelBloque, "r+");
	fseek(punteroArchivo, 0, SEEK_END);
	int tamanioFinal = ftell(punteroArchivo);

	if (tamanioFinal != tamanioBloque){
		string_append_with_format(&path, "%s.bin", listaBloques[indice]);
		fclose(punteroArchivo);
		free(pathDelBloque);
		break;
	}

	fclose(punteroArchivo);
	free(pathDelBloque);
	}

	int largoRegistro = string_length(contenidoDuplicado);
	tamanioArchivo += largoRegistro;

	if (tamanioArchivo <= (tamanioBloque * cantidadElementos)) {

		string_append(&bloques, "[");

		for (int i = 0; i < cantidadElementos; i++) {
			if (i == 0) {
				string_append_with_format(&bloques, "%s", listaBloques[i]);
			} else {
				string_append_with_format(&bloques, ",%s", listaBloques[i]);
			}
		}

		string_append(&bloques, "]");
		log_info(logger, "Contenido Registro %s", contenidoDuplicado);
		FILE* ficheroBloque = txt_open_for_append(path);
		txt_write_in_file(ficheroBloque, contenidoDuplicado);
		txt_close_file(ficheroBloque);
	}

	else {

		int posicionBloque;
		if((posicionBloque = buscarBloqueLibre()) == -1){
			log_error(logger, "No Hay Bloques Libres, Libere Espacio");
			free(bloques);
			free(path);
			string_iterate_lines(listaBloques, (void*) free);
			free(listaBloques);
			return;
		}

		int segundoResto = tamanioArchivo - (tamanioBloque * cantidadElementos);
		int primerResto = largoRegistro - segundoResto;
		log_info(logger, "Contenido Registro DUMP: %s - Resto1 %d - Resto2 %d", contenidoDuplicado, primerResto, segundoResto);

		char* primerExtracto = recortarExtractoPrimero(contenidoDuplicado ,primerResto);

		if(primerExtracto != NULL){
		FILE* ficheroBloque = txt_open_for_append(path);
		txt_write_in_file(ficheroBloque, primerExtracto);
		txt_close_file(ficheroBloque);
		}

		string_append(&bloques, "[");

		for (int i = 0; i < cantidadElementos; i++) {
			if (i == 0) {
				string_append_with_format(&bloques, "%s", listaBloques[i]);
			} else {
				string_append_with_format(&bloques, ",%s", listaBloques[i]);
			}
		}

		string_append_with_format(&bloques, ",%d", posicionBloque);
		string_append(&bloques, "]");

		char* rutaNuevaBloque = string_new();
		string_append(&rutaNuevaBloque, pathBloque);
		string_append_with_format(&rutaNuevaBloque, "%d.bin", posicionBloque);

		char* segundoExtracto = recortarExtractoSegundo(contenidoDuplicado ,segundoResto);

		if(segundoExtracto != NULL){

		FILE* otroFicheroBloque = txt_open_for_append(rutaNuevaBloque);
		txt_write_in_file(otroFicheroBloque, segundoExtracto);
		txt_close_file(otroFicheroBloque);
		}

		if(primerExtracto != NULL)  { free(primerExtracto);  }
		if(segundoExtracto != NULL) { free(segundoExtracto); }

		free(rutaNuevaBloque);
	}
	free(pathBloque);
	char* cadena1 = string_new();
	char* cadena2 = string_new();
	char* tamanio = string_itoa(tamanioArchivo);
	string_append_with_format(&cadena1, "SIZE=%s\n", tamanio);
	string_append_with_format(&cadena2, "BLOCKS=%s\n", bloques);

	FILE* nuevaParticion = fopen(pathParticion, "w+");
	fwrite(cadena1, 1, strlen(cadena1), nuevaParticion);
	fwrite(cadena2, 1, strlen(cadena2), nuevaParticion);
	fclose(nuevaParticion);

	free(contenidoDuplicado);
	free(contenidoRegistro);
	free(cadena1);
	free(cadena2);
	string_iterate_lines(listaBloques, (void*) free);
	free(listaBloques);
	free(bloques);
	free(tamanio);
	free(path);
}

void iniciarProcesoDUMP() {

	pthread_mutex_lock(&m_busqueda);
	pthread_mutex_lock(&m_inserts);
	if(list_size(listaDeMemTable) == 0){
		log_debug(logger, "No Se Encuentran Tablas Con Datos Para Iniciar El Proceso");
		pthread_mutex_unlock(&m_inserts);
		pthread_mutex_unlock(&m_busqueda);
		return;
	}

	int index = 0;

	t_memTable_LFS* nodoADumpear = list_get(listaDeMemTable, index);
	t_createdTables* tablaCreada = list_get(listadoTablas, index);

	while (nodoADumpear != NULL) {

			char* extensionArchivoDump = retornarDireccionPrincipalConTabla(nodoADumpear->nombreTabla);
			char letraExtension = retornarUltimoCaracter(
					nodoADumpear->nombreTabla);
			string_append_with_format(&extensionArchivoDump, "/%c%d.tmp",
					letraExtension, tablaCreada->contadorDumpeos);

			if (establecerBloqueYDatosAOcupar(extensionArchivoDump) == -1){
				log_error(logger, "No Hay Mas Bloques Para Persistir Los Datos Restantes");
				free(extensionArchivoDump);
				list_clean_and_destroy_elements(nodoADumpear->list_Table, (void*) eliminarRegistrosEnMem);
				free(nodoADumpear->list_Table);
				list_clean_and_destroy_elements(listaDeMemTable, (void*) eliminarDatosDeMemTable);
				pthread_mutex_unlock(&m_inserts);
				pthread_mutex_unlock(&m_busqueda);
				return;
			}

			int otroIndex = 0;
			data_list_Table* nodoRegistroDump = list_get(
					nodoADumpear->list_Table, otroIndex);

			while (nodoRegistroDump != NULL) {
				char* registro = string_new();

				string_append_with_format(&registro, "%s;%s;%s\n",
						nodoRegistroDump->timeStamp, nodoRegistroDump->key,
						nodoRegistroDump->value);
				insertarRegistroDentroDelBloque(registro, extensionArchivoDump);
				//free(registro);
				otroIndex++;
				nodoRegistroDump = list_get(nodoADumpear->list_Table,
						otroIndex);
			}

			log_info(logger,
					"Se Agregaron Los Registros A La Tabla %s Exitosamente",
					nodoADumpear->nombreTabla);

			free(extensionArchivoDump);
			list_clean_and_destroy_elements(nodoADumpear->list_Table, (void*) eliminarRegistrosEnMem);
			pthread_mutex_lock(&tablaCreada->mutex_tabla);
			tablaCreada->contadorDumpeos++;
			pthread_mutex_unlock(&tablaCreada->mutex_tabla);

		index++;
		nodoADumpear = list_get(listaDeMemTable, index);
		tablaCreada = list_get(listadoTablas, index);
	}
	list_clean_and_destroy_elements(listaDeMemTable, (void*) eliminarDatosDeMemTable);
	log_info(logger,
			"Se Crearon Los Archivos Temporales De Las Tablas Que Contenian Registros");
	pthread_mutex_unlock(&m_inserts);
	pthread_mutex_unlock(&m_busqueda);
}

void eliminarRegistrosEnMem(data_list_Table* elemento)
{
	free(elemento->timeStamp);
	free(elemento->key);
	free(elemento->value);
	free(elemento);
}

void eliminarDatosDeMemTable(t_memTable_LFS* elementoMem){
	free(elementoMem->nombreTabla);
	free(elementoMem);
}

void iniciarEnUnaTablaLaCOMPACTACION(char* nombreTabla) {

	int index = 0;
	t_createdTables* tablaCreada = list_get(listadoTablas, index);

	while (!(strstr(tablaCreada->nombreTabla, nombreTabla) != NULL)) {
		index++;
		tablaCreada = list_get(listadoTablas, index);
	}

	if(tablaCreada->contadorDumpeos == 1){
		log_info(logger, "No Se Encuentran Todavia Archivos Para Compactar");
		return;
	}

	pthread_mutex_lock(&tablaCreada->mutex_tabla);
	for (int otroIndex = 1; otroIndex < tablaCreada->contadorDumpeos;
			otroIndex++) {

		char* extensionDump = string_new();
		char* path = retornarDireccionPrincipalConTabla(tablaCreada->nombreTabla);
		char* nuevoPath = retornarDireccionPrincipalConTabla(tablaCreada->nombreTabla);
		char letraExtension = retornarUltimoCaracter(
				tablaCreada->nombreTabla);
		string_append_with_format(&path, "/%c%d.tmp", letraExtension,
				otroIndex);
		string_append_with_format(&extensionDump, "%c%d.tmp", letraExtension,
				otroIndex);
		string_append_with_format(&nuevoPath, "/%c%d.tmpc", letraExtension,
				otroIndex);

		int retorno = rename(path, nuevoPath);

		if (retorno != 0) {
			log_error(logger, "%s No Se Pudo Renombrar", extensionDump);
			return;
		}

		log_info(logger, "%s Renombrado Correctamente", extensionDump);
		free(extensionDump);
		free(path);
		free(nuevoPath);
		tablaCreada->cantidadCompactados++;
	}

	tablaCreada->contadorDumpeos = 1;
	pthread_mutex_unlock(&tablaCreada->mutex_tabla);

	log_info(logger,
			"Los Archivos Temporales De La %s Fueron Renombrados Exitosamente",
			nombreTabla);

	acotarTodosLosArchivosCompactadosDeLaTabla(nombreTabla, tablaCreada);
	log_info(logger,
			"Se Compactaron Los Archivos Satisfactoriamente Para La %s",
			tablaCreada->nombreTabla);
}

char* recortarExtractoPrimero(char* contenidoRegistro, int resto) {

	if(resto == 0){
		return NULL;
	}

	int largoRegistro = string_length(contenidoRegistro);
	int primerResultado = resto;
	char* primerExtracto = string_substring_until(contenidoRegistro, primerResultado);
	return primerExtracto;
}

char* recortarExtractoSegundo(char* contenidoRegistro, int resto) {

	if(resto == 0){
		return NULL;
	}

	int largoRegistro = string_length(contenidoRegistro);
	char cadena[largoRegistro];
	strcpy(cadena, contenidoRegistro);
	int contador = largoRegistro - resto;
	char* segundoExtracto = string_substring_from(contenidoRegistro, contador);
	return segundoExtracto;
}

void acotarTodosLosArchivosCompactadosDeLaTabla(char* nombreTabla, t_createdTables* nodoTabla) {

	t_list* almacenRegistros = list_create();
	int cantidadParticiones = 0;

	retornarCantidadParticionTabla(nombreTabla, &cantidadParticiones);

	descargarRegistrosALista(almacenRegistros, &cantidadParticiones,
			nombreTabla, nodoTabla);

	actualizacionDeRegistros(almacenRegistros, nombreTabla, nodoTabla);
	modificarParticionesOriginales(almacenRegistros, &cantidadParticiones,
			nombreTabla, nodoTabla);
	log_trace(logger, "Los Elementos Fueron Escritos En El Archivo");

	list_clean_and_destroy_elements(almacenRegistros, (void*) liberarElementos);
	list_destroy(almacenRegistros);
}

void descargarRegistrosALista(t_list* almacenRegistros ,int* cantidadParticiones ,char* nombreTabla, t_createdTables* nodoTabla) {

	for (int index = 1; index < (*cantidadParticiones + 1); index++) {

		list_Particiones* listaParticion = malloc(sizeof(list_Particiones));

		listaParticion->numeroParticion = index;

		listaParticion->nodosParticion = list_create();
		list_add(almacenRegistros, listaParticion);

		char* path = retornarDireccionPrincipalConTabla(nombreTabla);
		string_append_with_format(&path, "/%d.bin", index);

		t_config* punteroConfig = config_create(path);

		char** listaBloques = config_get_array_value(punteroConfig, "BLOCKS");

		int cantidadElementos = retornarCantidadDeBlocksEnParticion(listaBloques);

		for(int i = 0; i < cantidadElementos; i++){

			char* nuevoPath = retornarDireccionBloques();
			char* numeroBloque = listaBloques[i];
			string_append_with_format(&nuevoPath, "%s.bin", numeroBloque);

			FILE* punteroBloque = fopen(nuevoPath, "r+");

			list_Particiones* nodo = list_get(almacenRegistros, (index-1));

			meterRegistrosEnLaLista(nodo, punteroBloque);

			pthread_mutex_lock(&m_bitarray);
			bitarray_clean_bit(bitarray, atoi(numeroBloque));
			pthread_mutex_unlock(&m_bitarray);

			fclose(punteroBloque);
			recrearBloqueVacio(nuevoPath);

			free(nuevoPath);
		}

		string_iterate_lines(listaBloques, (void*) free);
		free(listaBloques);
		config_destroy(punteroConfig);

		pthread_mutex_lock(&nodoTabla->mutex_tabla);
		remove(path);
		FILE* punteroArchivo = fopen(path, "w+");
		fclose(punteroArchivo);
		pthread_mutex_unlock(&nodoTabla->mutex_tabla);
		free(path);
	}
}

void actualizacionDeRegistros(t_list* almacenRegistros, char* nombreTabla, t_createdTables* nodoTabla) {

	for (int otroIndex = 1; otroIndex < nodoTabla->cantidadCompactados;
			otroIndex++) {

		char* lineaLeida = NULL;
		size_t buffersize = 0;

		char* extensionCompactacion = string_new();
		char* path = retornarDireccionPrincipalConTabla(nombreTabla);
		char letraExtension = retornarUltimoCaracter(
				nodoTabla->nombreTabla);
		string_append_with_format(&path, "/%c%d.tmpc", letraExtension,
				otroIndex);
		string_append_with_format(&extensionCompactacion, "%c%d.tmpc",
				letraExtension, otroIndex);

		log_info(logger, "Path: %s", path);
		t_config* configArchivo = config_create(path);
		if(configArchivo == NULL) { log_info(logger, "No Se Pudo Abrir El Archivo"); }
		char** listBloques = config_get_array_value(configArchivo, "BLOCKS");
		int cantidadElementos = retornarCantidadDeBlocksEnParticion(listBloques);
		config_destroy(configArchivo);

		char* lineaIncompleta = NULL;
		for(int i = 0; i < cantidadElementos; i++){

			char* numeroBloque = listBloques[i];
			char* direccionBloque = retornarDireccionBloques();
			string_append_with_format(&direccionBloque, "%s.bin", numeroBloque);

			FILE* punteroArchivo = fopen(direccionBloque, "r+");

		while (getline(&lineaLeida, &buffersize, punteroArchivo) != -1) {
			char* lineaDuplicada = string_duplicate(lineaLeida);

				if (lineaIncompleta == NULL) {
					int largoCadena = string_length(lineaDuplicada);
					if (lineaDuplicada[largoCadena - 1] != '\n') {
						lineaIncompleta = string_duplicate(lineaDuplicada);
						free(lineaDuplicada);
						break;
					}
				}

				if (lineaIncompleta != NULL) {
					int largoCadena = string_length(lineaDuplicada);
					if (lineaDuplicada[largoCadena - 1] == '\n') {
						string_append(&lineaIncompleta, lineaDuplicada);
						free(lineaDuplicada);
						lineaDuplicada = string_duplicate(lineaIncompleta);
						free(lineaIncompleta);
						lineaIncompleta = NULL;
					}
				}

			char* otroDuplicado = string_duplicate(lineaDuplicada);
			char* registroTimestamp = strtok(lineaDuplicada, ";");
			char* registroKey = strtok(NULL, ";");
			char* registroValue = strtok(NULL, ";");
			bool valorNoEncontrado = 1;

			int keyNumerico = atoi(registroKey);
			int moduloResto = keyNumerico % list_size(almacenRegistros);
			list_Particiones* elementoLista = list_get(almacenRegistros, moduloResto);

			int indice = 0;
			char* registroParticion = list_get(elementoLista->nodosParticion, indice);

			while (registroParticion != NULL) {
				char* duplicadoRegistro = string_duplicate(registroParticion);

				char* registroEnListaTimeStamp = strtok(duplicadoRegistro, ";");
				char* registroEnListaKey = strtok(NULL, ";");
				char* registroEnListaValue = strtok(NULL, ";");

				if(registroKey == NULL && registroEnListaKey == NULL) {

				}

				else {
				int keyValue = atoi(registroKey);
				int keyRegistro = atoi(registroEnListaKey);

				if (keyValue == keyRegistro) {

					unsigned timestampValue = atoi(registroTimestamp);
					unsigned timestampRegistro = atoi(registroEnListaTimeStamp);

					if (timestampValue > timestampRegistro) {

						char* elementoRetornado = list_remove(elementoLista->nodosParticion, indice);
						free(elementoRetornado);
						registroParticion = otroDuplicado;
						list_add(elementoLista->nodosParticion, registroParticion);
//						list_add_in_index(elementoLista->nodosParticion, indice, registroParticion);
						valorNoEncontrado = 0;
						free(duplicadoRegistro);
						break;
					}
					free(duplicadoRegistro);
					free(otroDuplicado);
					valorNoEncontrado = 0;
				    break;
				}
				}
				free(duplicadoRegistro);
				indice++;
				registroParticion = list_get(elementoLista->nodosParticion, indice);
			}

			if (valorNoEncontrado) { list_add(elementoLista->nodosParticion, otroDuplicado);}
			free(lineaDuplicada);
		}

		fclose(punteroArchivo);
		recrearBloqueVacio(direccionBloque);

		pthread_mutex_lock(&m_bitarray);
		bitarray_clean_bit(bitarray, atoi(numeroBloque));
		pthread_mutex_unlock(&m_bitarray);

		free(direccionBloque);
		}
		pthread_mutex_lock(&nodoTabla->mutex_tabla);
		remove(path);
		pthread_mutex_unlock(&nodoTabla->mutex_tabla);
		free(path);
		log_info(logger, "Se Removio El Archivo %s De La %s",
		extensionCompactacion, nodoTabla->nombreTabla);
		free(extensionCompactacion);
		string_iterate_lines(listBloques, (void*) free);
		free(listBloques);
		free(lineaLeida);
	}
	nodoTabla->cantidadCompactados = 1;
}

void meterRegistrosEnLaLista(list_Particiones* listaRegistros, FILE* punteroBloque) {

	char* lineaLeida = NULL;
	size_t buffersize = 0;
	int contador = 0;

	while (getline(&lineaLeida, &buffersize, punteroBloque) != -1) {

		char* duplicado = string_duplicate(lineaLeida);

		if (list_is_empty(listaRegistros->nodosParticion)){
			list_add(listaRegistros->nodosParticion, duplicado);
		}

		else {
			int cantidadRegistros = list_size(listaRegistros->nodosParticion);
			char* ultimoRegistro = list_get(listaRegistros->nodosParticion,
					cantidadRegistros - 1);
			int largoCadena = string_length(ultimoRegistro);
			if (ultimoRegistro[largoCadena - 1] != '\n') {
				string_append(&ultimoRegistro, duplicado);
				char* elementoRemovido = list_remove(listaRegistros->nodosParticion,
						cantidadRegistros - 1);
				list_add(listaRegistros->nodosParticion, ultimoRegistro);

			} else {
				list_add(listaRegistros->nodosParticion, duplicado);
			}
		}
	}
	free(lineaLeida);
}

void modificarParticionesOriginales(t_list* almacenRegistros,
		int* cantidadParticiones, char* nombreTabla, t_createdTables* nodoTabla) {

		char* pathMetadata = retornarDireccionMetadata();

		t_config* configMetadata = config_create(pathMetadata);

		if(configMetadata == NULL){
			log_error(logger, "No Existe La Metadata De %s", nombreTabla);
			free(pathMetadata);

		}

		int tamanioBloques = config_get_int_value(configMetadata, "BLOCK_SIZE");
		config_destroy(configMetadata);

		free(pathMetadata);

	for(int i = 1; i < (*cantidadParticiones + 1); i++){

		list_Particiones* elementoNodo = list_get(almacenRegistros, (i - 1));
		double totalSizeFile = devolverTamanioTotalDeArchivo(elementoNodo);

		double cantidadBloques = totalSizeFile / tamanioBloques;
		int bloquesAReservar = (int) ceil(cantidadBloques);

		if(bloquesAReservar == 0) { bloquesAReservar++; }

		int almacenBloques[bloquesAReservar];
		reservarArrayBloques(almacenBloques, bloquesAReservar);

		int indiceBloque = 0;
		int tamanioRegistro = 0;

		int index = 0;
		char* registro = list_get(elementoNodo->nodosParticion, index);

		while(registro != NULL){

		int largoRegistro = string_length(registro);
		tamanioRegistro += largoRegistro;

		char* pathBloques = retornarDireccionBloques();
		char* bloquesChar = string_itoa(almacenBloques[indiceBloque]);
		string_append_with_format(&pathBloques, "%s.bin", bloquesChar);
		free(bloquesChar);

		if(tamanioRegistro <= tamanioBloques){
			FILE* ficheroBloque = txt_open_for_append(pathBloques);
			txt_write_in_file(ficheroBloque, registro);
			txt_close_file(ficheroBloque);
		}

		else{

			int segundoResto = tamanioRegistro - tamanioBloques;
			int primerResto = largoRegistro - segundoResto;

			char* primerExtracto = recortarExtractoPrimero(registro ,primerResto);

			if(primerExtracto != NULL){
			FILE* ficheroBloque = txt_open_for_append(pathBloques);
			txt_write_in_file(ficheroBloque, primerExtracto);
			txt_close_file(ficheroBloque);
			free(primerExtracto);
			}

			indiceBloque++;
			char* otroPathBloques = retornarDireccionBloques();
			char* nuevosBloquesChar = string_itoa(almacenBloques[indiceBloque]);
			string_append_with_format(&otroPathBloques, "%s.bin", nuevosBloquesChar);
			free(nuevosBloquesChar);

			tamanioRegistro = 0;
			tamanioRegistro += segundoResto;

			char* segundoExtracto = recortarExtractoSegundo(registro ,segundoResto);

			if(segundoExtracto != NULL){
			FILE* otroFicheroBloque = txt_open_for_append(otroPathBloques);
			txt_write_in_file(otroFicheroBloque, segundoExtracto);
			txt_close_file(otroFicheroBloque);
			free(segundoExtracto);
			}

			free(otroPathBloques);
		}
		free(pathBloques);
		index++;
		registro = list_get(elementoNodo->nodosParticion, index);
		}

		char* bloques = string_new();
		string_append(&bloques, "[");

		for (int i = 0; i < bloquesAReservar; i++) {
			if (i == 0) {
				string_append_with_format(&bloques, "%d", almacenBloques[i]);
			} else {
				string_append_with_format(&bloques, ",%d", almacenBloques[i]);
			}
		}

		string_append(&bloques, "]");

		char* direccionTabla = retornarDireccionPrincipalConTabla(nombreTabla);
		string_append_with_format(&direccionTabla, "/%d.bin", i);
		pthread_mutex_lock(&nodoTabla->mutex_tabla);
		FILE* punteroParticion = fopen(direccionTabla, "w+");
		fprintf(punteroParticion, "SIZE=%d\n", (int)totalSizeFile);
		fprintf(punteroParticion, "BLOCKS=%s\n", bloques);
		fclose(punteroParticion);
		pthread_mutex_unlock(&nodoTabla->mutex_tabla);
		free(bloques);
		free(direccionTabla);
	}
}
//-------------------------------------------------------------------------------//

//Otras Funciones
void mostrarInfoTabla(char* path, char* nombreTabla) {

	t_config* configuracionALeer = config_create(path);

	if(configuracionALeer == NULL) {
		log_error(logger, "No Se Pudo Abrir La Metadata");
		return;
	}

	char* sentenciaConsistencia = config_get_string_value(configuracionALeer,
			"CONSISTENCY");
	int sentenciaParticiones = config_get_int_value(configuracionALeer,
			"PARTITIONS");
	int sentenciaCompactacion = config_get_int_value(configuracionALeer,
			"COMPACTION_TIME");

	log_trace(logger, "La Metadata De %s Es La Siguiente:", nombreTabla);
	log_info(logger, "CONSISTENCY = %s", sentenciaConsistencia);
	log_info(logger, "PARTITIONS = %d", sentenciaParticiones);
	log_info(logger, "COMPACTION_TIME = %d\n", sentenciaCompactacion);

	config_destroy(configuracionALeer);

}

int calculoModuloParticion(int cantPart, int keyValue) {
	return (keyValue % cantPart) + 1;
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
						sem_wait(&iNotify);
						char* copiaPath = string_new();
						string_append(&copiaPath, path);
						string_append(&copiaPath, "/lissandra.config");
						log_info(logger, "The file %s was modified.\n",
								event->name);
						t_config* archivo_Config = config_create(copiaPath);
						datosConfigLissandra->retardo = config_get_int_value(
								archivo_Config, "RETARDO");
						datosConfigLissandra->tiempoDump = config_get_int_value(
								archivo_Config, "TIEMPO_DUMP");
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
	return;
}

double devolverTamanioTotalDeArchivo(list_Particiones* elementoNodo){
	double acumuladorTamanio = 0;

	int index = 0;
	char* registroObtenido = list_get(elementoNodo->nodosParticion, index);

	while(registroObtenido != NULL){
		int tamanioRegistro = string_length(registroObtenido);
		acumuladorTamanio += tamanioRegistro;
		index++;
		registroObtenido = list_get(elementoNodo->nodosParticion, index);
	}
	return acumuladorTamanio;
}
//-------------------------------------------------------------------------------//

//Funciones De Comunicacion
void identificarProceso(socket_connection * connection ,char** args){
     log_info(logger,"Se Ha Conectado %s En El Socket NRO %d  Con IP %s, PUERTO %d\n", args[0],connection->socket,connection->ip,connection->port);
     char* valueString = string_itoa(datosConfigLissandra->tamanioValue);
     runFunction(connection->socket, "comunicarTamanioValue", 1, valueString);
     free(valueString);
}

void pasarDescribePorId(socket_connection* connection, char** args){

	char* listadoTablas = recorridoDeTodasLasTablasPersistidas();

	if(listadoTablas == NULL) {
		runFunction(connection->socket, "resultadoAlIdentificarseConKernel", 1, "");
	}
	else{
		runFunction(connection->socket, "resultadoAlIdentificarseConKernel", 1, listadoTablas);
	}
	free(listadoTablas);
}

void realizarSelectPorComunicacion(socket_connection* connection, char** args){
	datos_Select* infoSelect = malloc(sizeof(datos_Select));
	infoSelect->nombreTabla = args[0];
	infoSelect->key = args[1];
	seleccionarYEncontrarUltimaKeyActualizada(connection, infoSelect);
	free(infoSelect);
}

void realizarInsertPorComunicacion(socket_connection* connection, char** args){
	datos_Insert* infoInsert = malloc(sizeof(datos_Insert));
	infoInsert->nombreTabla = args[0];
	infoInsert->key = string_duplicate(args[1]);
	infoInsert->value = string_duplicate(args[2]);
	infoInsert->timestamp = string_duplicate(args[3]);
	log_info(logger," insert de memoria key _%s_ value _%s_ timestamp _%s_", infoInsert->key, infoInsert->value, infoInsert->timestamp);
	insertarValoresDentroDeLaMemTable(connection, infoInsert);
	//free(infoInsert);
}

void realizarCreatePorComunicacion(socket_connection* connection, char** args){
	datos_Create* infoCreate = malloc(sizeof(datos_Create));
	infoCreate->nombreTabla = string_duplicate(args[0]);
	infoCreate->tipoConsistencia = args[1];
	infoCreate->numeroDeParticiones = args[2];
	infoCreate->tiempoDeCompactacion = args[3];
	crear_y_almacenar_datos(connection, infoCreate);
	free(infoCreate->nombreTabla);
	free(infoCreate);
}

//Mensaje Cuando Memoria Se Lo Indica Desde Su Consola
void mensajeCreateDeMemoria(socket_connection* connection, char** args){
	datos_Create* infoCreate = malloc(sizeof(datos_Create));
	infoCreate->nombreTabla = args[0];
	infoCreate->tipoConsistencia = args[1];
	infoCreate->numeroDeParticiones = args[2];
	infoCreate->tiempoDeCompactacion = args[3];
	crear_y_almacenar_datos(NULL, infoCreate);
	free(infoCreate);
}

void realizarDescribePorComunicacion(socket_connection* connection, char** args){

	if(!string_contains(args[0], "GLOBAL")){
		recorridoDeUnaTabla(connection, args[0]);
	}
	else {
		log_debug(logger, "Me Llego Un Describe Global");
		char* listadoTablas = recorridoDeTodasLasTablasPersistidas();
		if(listadoTablas == NULL){ runFunction(connection->socket, "respuestaSobreDescribe", 2, "DESCRIBE", "0");
								   free(listadoTablas);
								   return;}
		runFunction(connection->socket, "pasarDescribeGlobalEnUno", 2, "1", listadoTablas);
		free(listadoTablas);
	}
}

void mensajeDescribeDeMemoria(socket_connection* connection, char** args){

	if (!string_contains(args[0], "GLOBAL")) {
		recorridoDeUnaTabla(NULL, args[0]);
	} else {
		log_debug(logger, "Me Llego Un Describe Global");
		char* listadoTablas = recorridoDeTodasLasTablasPersistidas();
		free(listadoTablas);
	}
}

//args[0]: NombreTabla
void realizarDropPorComunicacion(socket_connection* connection, char** args){
	destruirEnCascadaElDirectorio(connection, args[0]);
}

void mensajeDropDeMemoria(socket_connection* connection, char** args){
	destruirEnCascadaElDirectorio(NULL, args[0]);
}

void disconnect(socket_connection* socketInfo) {
	log_info(logger, "El socket n°%d se ha desconectado.", socketInfo->socket);
	free(socketInfo->ip);
	free(socketInfo);
}
//-------------------------------------------------------------------------------//

//Funciones De Liberacion De Datos
void liberarReferenciasDeTablas(int cantidadTablas) {
	for (int i = 0; i < cantidadTablas; i++) {
		free(resultados[i]);
		resultados[i] = NULL;
	}
}

void liberarMapeoEnMemoria(){
		if (munmap(bitarray->bitarray, (datosFileSystem->cantidadBloques)/8) == -1) {
			log_error(logger, "Error Un-Mapping The File");
		}
}

void liberarDatos(t_memTable_LFS *self){
	int cantidadElementos = list_size(self->list_Table);

	if(cantidadElementos != 0){
	list_clean_and_destroy_elements(self->list_Table, (void*) liberarRegistrosEnMemTable);
	free(self->nombreTabla);
	free(self->list_Table);
	}
	free(self);
}

void table_destroy(t_memTable_LFS *self) {

	int index = 0;
	int cantidadElementos = list_size(self->list_Table);

	if (cantidadElementos == 0) {
		free(self->list_Table);
		free(self);
		return;
	}

	while (index < cantidadElementos) {

		list_clean_and_destroy_elements(self->list_Table, (void*) free);
		index++;
	}

	free(self->list_Table);
	free(self);
}

void liberarDatosDelMetadata(t_config_Lissandra* datosConfigLissandra) {
	free(datosConfigLissandra->ptoMontaje);
	free(datosConfigLissandra);
}

void liberarDatosFileSystem(t_data_FS* datosFileSystem) {
	free(datosFileSystem);
}

//Funciones Para Liberar Datos
void duplicados_destroy(void* self) {
	free(self);
}

void liberarElementos(list_Particiones* self){

	int cantidadElementos = list_size(self->nodosParticion);

	if (cantidadElementos == 0) {
		free(self->nodosParticion);
		free(self);
		return;
	}

	list_clean_and_destroy_elements(self->nodosParticion, (void*) free);
	free(self->nodosParticion);
	free(self);
}

void liberarSiOcupaDatos(char* self){

	if(self != NULL){ free(self); }
}

void liberarRegistrosEnMemTable(data_list_Table* registro){

	free(registro->key);
	free(registro->value);
	free(registro->timeStamp);
	free(registro);
}
//-------------------------------------------------------------------------------//
