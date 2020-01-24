#include "consolaMemoria.h"
#include "libMemoria.h"

void consolaMemoria(){

	char* linea = NULL;
	char* valor; // deprecated
	char espaciosEnBlanco[4]=" \n\t";
	const char comillas = '"';
	int continuar = 1;

	char* primeraPalabra = NULL;
	char* segundaPalabra = NULL;
	char* terceraPalabra = NULL;
	char* cuartaPalabra = NULL;
	char* quintaPalabra = NULL;
	t_list* sentenciasEscritas = list_create();

	log_info(logger, "Bienvenido A La Consola");

	do{
		log_info(logger, "Ingrese Una Sentencia");
		linea = readline(">");

		string_to_upper(linea);
		primeraPalabra = strtok(linea, espaciosEnBlanco);

		palabraReservada = transformarStringToEnum(primeraPalabra);

		if(palabraReservada == INSERT){
			segundaPalabra = strtok(NULL, espaciosEnBlanco); //validar nombre de tabla
			terceraPalabra = strtok(NULL, espaciosEnBlanco);
			cuartaPalabra = strtok(NULL, &comillas);
			quintaPalabra = strtok(NULL, &comillas); //validar key
		}

		switch(palabraReservada){

		case SELECT: //ejemplo: SELECT TABLA1 111

				segundaPalabra = strtok(NULL, espaciosEnBlanco);
				terceraPalabra = strtok(NULL, espaciosEnBlanco);
				select_mem_struct = malloc(sizeof(SELECT_MEMORIA_t));
				select_mem_struct->tabla = segundaPalabra;
				select_mem_struct->key = terceraPalabra;
				SELECT_MEMORIA(select_mem_struct, NULL);
				sleep(datosConfigMemoria->retardoMemoria);
				free(select_mem_struct);
				break;

		case INSERT: //ejemplo: INSERT TABLA1 111 "VALOR" 123

				insert_mem_struct = malloc(sizeof(INSERT_MEMORIA_t));
				insert_mem_struct->tabla = segundaPalabra;
				insert_mem_struct->key = terceraPalabra;
				insert_mem_struct->valor = cuartaPalabra;
				insert_mem_struct->timestamp = getCurrentTime();
				insert_mem_struct->connection = NULL;

				INSERT_MEMORIA(insert_mem_struct);
				sleep(datosConfigMemoria->retardoMemoria);
				free(insert_mem_struct);
				break;

		case CREATE:
				segundaPalabra = strtok(NULL, espaciosEnBlanco);
				terceraPalabra = strtok(NULL, espaciosEnBlanco);
				cuartaPalabra = strtok(NULL, espaciosEnBlanco);
				quintaPalabra = strtok(NULL, espaciosEnBlanco);
				sleep(datosConfigMemoria->retardoFS);
				runFunction(socketLFS, "mensajeCrearDeMemoria", 4, segundaPalabra, terceraPalabra, cuartaPalabra, quintaPalabra);
				break;

		case JOURNAL:
				JOURNAL_MEMORIA(NULL);
				sleep(datosConfigMemoria->retardoMemoria);
				bitarrayImprimir1eros100();
				break;

		case DESCRIBE:

			sleep(datosConfigMemoria->retardoFS);
			if (segundaPalabra == NULL) {
				runFunction(socketLFS, "mensajeDescribeDeMemoria", 1, "GLOBAL");
			}
			else {
				runFunction(socketLFS, "mensajeDescribeDeMemoria", 1,
						segundaPalabra);
			}
			break;

		case DROP:
				segundaPalabra = strtok(NULL, espaciosEnBlanco);
				DROP_MEMORIA(segundaPalabra, NULL);
				sleep(datosConfigMemoria->retardoMemoria);
				runFunction(socketLFS, "mensajeDropeoDeMemoria", 1, segundaPalabra);
				break;

		case SALIR:
				continuar = strcmp(linea, "SALIR");
				if(continuar) printf("comando no reconocido\n");
				break;
		default:
				break;
		}
		list_add(sentenciasEscritas, linea);
	} while(continuar);
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

			else if (string_contains(palReservada, "JOURNAL")) {
				palabraTransformada = JOURNAL;
			}

			else if (string_contains(palReservada, "SALIR")) {
				palabraTransformada = SALIR;
			}

			return palabraTransformada;
		}

void planificarGossip(){

	//Primera Vez Que Lo Hace
	realizarGossip();
	unsigned inicioDeGossip = (unsigned) time(NULL);
	int tiempoGossipMecanico = datosConfigMemoria->retardoGossiping;
	int valorRetardo = datosConfigMemoria->retardoMemoria;

	while(flagSalida){

		sleep(tiempoGossipMecanico/1000);
		sem_wait(&iNotify);
		if(tiempoGossipMecanico != datosConfigMemoria->retardoGossiping){
			tiempoGossipMecanico = datosConfigMemoria->retardoGossiping;
		}
		sem_post(&iNotify);
		realizarGossip();
	}

	log_trace(logger,"Se Sale Del Programa Del Gossip");
}

void agregarDireccionPropiaAGossiping(){

	createListen(datosConfigMemoria->puertoMemoria, NULL, llamadaFunciones, &disconnect, NULL);

	char hostbuffer[256];
	char* ip = NULL;
	struct sockaddr_in host;

	host.sin_family = AF_INET;
	host.sin_port = htons(datosConfigMemoria->puertoMemoria);
	host.sin_addr.s_addr = INADDR_ANY;

	memset(&(host.sin_zero), '\0', 8);

	gethostname(hostbuffer, sizeof(hostbuffer));
	gethostbyname(hostbuffer);
	ip = inet_ntoa(host.sin_addr);

	nodoGossiping_t* nuevoRegistroGossip = malloc(sizeof(nodoGossiping_t));
	nuevoRegistroGossip->memoryNumber = string_duplicate(datosConfigMemoria->memoryNumber);
	nuevoRegistroGossip->ip = ip;
	nuevoRegistroGossip->puerto = datosConfigMemoria->puertoMemoria;

	pthread_mutex_lock(&m_agregarMemoria);
	list_add(listaDeGossiping,nuevoRegistroGossip);
	pthread_mutex_unlock(&m_agregarMemoria);
	log_info(logger, "Se Agrego Mi Memory Number: %s - IP: %s - Puerto: %d",
			nuevoRegistroGossip->memoryNumber, ip, nuevoRegistroGossip->puerto);

}

bool esTiempoDeEjecutarse(unsigned inicioTiempo, unsigned nuevoTiempo, int tiempoACumplir) {
	return (nuevoTiempo - inicioTiempo) == tiempoACumplir;
}

char* enviarTablaGossipEnUno(){

	int cantidadTablas = list_size(listaDeGossiping);
	char* sentenciasGossip = string_new();

	int i = 0;
	nodoGossiping_t* elementoMemoria = list_get(listaDeGossiping, i);

	while(elementoMemoria != NULL){

		if(i == 0){
			string_append_with_format(&sentenciasGossip, "%s %s %d", elementoMemoria->memoryNumber, elementoMemoria->ip, elementoMemoria->puerto);
		}

		else{
			string_append_with_format(&sentenciasGossip, ",%s %s %d", elementoMemoria->memoryNumber, elementoMemoria->ip, elementoMemoria->puerto);
		}

		i++;
		elementoMemoria = list_get(listaDeGossiping, i);
	}
	return sentenciasGossip;
}

void recibirMensajeParaGossip(socket_connection* connection, char** args){

	log_info(logger, "Recibi De %s Socket: %d Para Pasar Mi Gossip", args[0], connection->socket);

	char* listaGossip = enviarTablaGossipEnUno();

	runFunction(connection->socket, "recibirTablaGossip", 1, listaGossip);

//	puertoSocket = malloc(sizeof(conexionGossip_t));
//	puertoSocket->puerto = connection->port;
//	puertoSocket->socketConexion = connection->socket;

	runFunction(connection->socket, "pasarTambienInfoGossip", 1, "Memoria");
	free(listaGossip);
}

void procesarInfoGossip(socket_connection* connection, char** args){

	char** listadoMemorias = string_split(args[0], ",");

	int indice = 0;
	while (listadoMemorias[indice] != NULL) {
		char** tablaComponentes = string_n_split(listadoMemorias[indice], 3,
				" \n\t");
		nodoGossiping_t* tablaRecibida = malloc(sizeof(nodoGossiping_t));
		tablaRecibida->memoryNumber = string_duplicate(tablaComponentes[0]);
		tablaRecibida->socket = -1;
		tablaRecibida->ip = string_duplicate(tablaComponentes[1]);
		tablaRecibida->puerto = atoi(tablaComponentes[2]);
		agregarMemoriaAListaDeGossiping(tablaRecibida, connection);
		string_iterate_lines(tablaComponentes, (void*) free);
		free(tablaComponentes);
		indice++;
	}

	string_iterate_lines(listadoMemorias, (void*) free);
	free(listadoMemorias);

	sem_post(&esperaGossip);
}



void agregarMemoriaAListaDeGossiping(nodoGossiping_t* memoriaNueva, socket_connection* connection){


	bool sonElMismoNumero(nodoGossiping_t* memoriaAComparar){
		return memoriaNueva->puerto == memoriaAComparar->puerto;
	}

	nodoGossiping_t* memoriaEncontrada = list_find(listaDeGossiping, (void*) sonElMismoNumero);

	if(memoriaEncontrada != NULL){
		if(memoriaEncontrada->memoryNumber == NULL){
			memoriaEncontrada->memoryNumber = memoriaNueva->memoryNumber;
		}

		log_error(logger, "El Numero De Memoria %s Ya Existe", memoriaNueva->memoryNumber);
		return;
	}

	pthread_mutex_lock(&m_agregarMemoria);
	if(memoriaNueva->socket == -1){
		int conexion = connectServer(memoriaNueva->ip, memoriaNueva->puerto, llamadaFunciones, &disconnect, NULL);
		memoriaNueva->socket = conexion;
	}

	list_add(listaDeGossiping, memoriaNueva);
	pthread_mutex_unlock(&m_agregarMemoria);
	log_info(logger, "Se Agrego El Memory Number %s Con Socket %d A La Lista", memoriaNueva->memoryNumber, memoriaNueva->socket);
}

void envioGossip(socket_connection* connection, char** args){

	char* listaGossip = enviarTablaGossipEnUno();
	runFunction(connection->socket, "recibirTablaGossip", 2, listaGossip, "1");
	free(listaGossip);
}
