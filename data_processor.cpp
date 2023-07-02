#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <bson.h>
#include <mongoc.h>
#include "json.hpp" 
#include "mqtt/client.h" 
#include <map>
#include <deque>
#include <ctime>


#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

std::string aux_sensor_id;
std::string aux_machine_id;
std::string aux_data;

bool recebeu = false; //var auxiliar indicando que houve mensagem recebida
bool alarme = true; //var iniciar thread que acompanha alarme

int teste_media = 0; //var auxiliar para teste alarme média

std::map<std::string, std::deque<float>> sensor_datas; // armazena os dados de cada sensor

std::map<std::string,std::string> last_data; // armazena última data da última data 

std::map<std::string,int> intervalo_sensor; //armazena intervalos de cada sensor

std::map<std::string,int> alarmes;//

std::vector<std::thread> threads; //vetor de threads

const int MAX_ELEMENTOS = 10; //define tamanho de datas para calcular a média

void insert_document(mongoc_collection_t *collection, std::string machine_id, std::string timestamp_str, int value) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;
    
    std::tm tm{};
    std::istringstream ss{timestamp_str};
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    std::time_t time_t_timestamp = std::mktime(&tm);

    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machine_id", machine_id.c_str());
    BSON_APPEND_TIME_T(doc, "timestamp", time_t_timestamp);
    BSON_APPEND_INT32(doc, "value", value);

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        std::cerr << "Failed to insert doc: " << error.message << std::endl;
    }

    bson_destroy(doc);
}

void insert_document_alarme(mongoc_collection_t *collection, std::string machine_id, std::string sensor_id, std::string description) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;
    
    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machine_id", machine_id.c_str());
    BSON_APPEND_UTF8(doc, "sensor_id", sensor_id.c_str());
    BSON_APPEND_UTF8(doc, "description", description.c_str());

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        std::cerr << "Failed to insert doc: " << error.message << std::endl;
    }
    bson_destroy(doc);
}

void insert_document_initial(mongoc_collection_t *collection, std::string machine_id, std::string sensor_id, int intervalo) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;
    
    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machine_id", machine_id.c_str());
    BSON_APPEND_UTF8(doc, "sensor_id", sensor_id.c_str());
    BSON_APPEND_INT32(doc, "intervalo", intervalo);

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        std::cerr << "Failed to insert doc: " << error.message << std::endl;
    }
    bson_destroy(doc);
}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}


bool media(float value, std::string machine_id, std::string sensor_id) {
    float sum = 0.0;
    int tam = 0;
    if (sensor_datas[sensor_id].size() < 6) {       //só calcula a média caso haja 6 datas no mínimo
        return false;
    }
    for (const auto& data_value : sensor_datas[sensor_id]) {
        sum = sum + data_value;
        tam++;
    }
    float media = sum / tam;
    if (value < 0.8 * media || value > 1.2 * media) {
        return true;
    }
    return false;
}

std::string convertToISOString(const std::tm& timeStruct) {
    std::ostringstream oss;
    oss << std::put_time(&timeStruct, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

std::time_t convertToTimeT(const std::string& dateString) {
    std::tm timeStruct = {};
    std::istringstream ss(dateString);
    ss >> std::get_time(&timeStruct, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        // Tratamento de erro, se necessário
    }
    std::time_t time = std::mktime(&timeStruct);
    return time;
}

bool tempo_inativo(const std::string& machine_id, const std::string& sensor_id) {
    auto time_now = std::chrono::system_clock::now();
    std::time_t time_now_t = std::chrono::system_clock::to_time_t(time_now);
    std::tm* time_now_tm = std::gmtime(&time_now_t);
    std::string isoString = convertToISOString(*time_now_tm);
    // Convertendo o valor armazenado em last_data[sensor_id] para um objeto de tempo

    std::time_t time1 = convertToTimeT(last_data[sensor_id]);
    std::time_t time2 = convertToTimeT(isoString);

    std::time_t diff = time2 - time1;

    int diffInSeconds = static_cast<int>(diff);

    // Verificando se o sensor está inativo
    if (diffInSeconds > intervalo_sensor[sensor_id]) {
        // Fazer algo quando o sensor está inativo
        return true;
    }

    return false;
}


void alarmes_verified(){
    std::string clientId = "clientId_alarmes";
    mqtt::client client(BROKER_ADDRESS, clientId);
    
    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "alarmes");
    
         // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *dbbb;

    public:
        callback(mongoc_database_t *dbbb) : dbbb(dbbb) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string alarme_name = "alarmes";
            std::string machine_id = j["machine_id"];
            std::string sensor_id = j["sensor_id"];
            std::string alarme_descr = j["description"];

            std::cout << "Machine_id: " << machine_id << std::endl;
            std::cout << "Sensor_id: " << sensor_id << std::endl;
            std::cout << "Descricao: " << alarme_descr  << std::endl;

            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(dbbb, alarme_name.c_str());
            insert_document_alarme(collection, machine_id, sensor_id, alarme_descr);
            mongoc_collection_destroy(collection);
        }
    };
    callback cb(database);
    client.set_callback(cb);
    

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    std::string topic;
    topic = "/alarmes";
    //topic = "sensor/disk";
    
    try {
        client.connect(connOpts);
        client.subscribe(topic, QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        //return EXIT_FAILURE;
    }
    
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  
    //return EXIT_SUCCESS;

}

void main_sensor(std::string sensor_id, std::string machine_id, std::string data_type){

    std::string sensor_id_parametro = sensor_id;
    std::string machine_id_parametro = machine_id;
    std::string data_parametro = data_type;
    std::string description;
    alarmes[sensor_id] = 0;

    std::cout << "Sensor_id: " << sensor_id_parametro << std::endl;
    std::cout << "Data_type: " << data_parametro << std::endl;
    std::cout << "Machine_id: " << machine_id_parametro << std::endl;

    std::string clientId = "clientId" + sensor_id_parametro;
    mqtt::client client(BROKER_ADDRESS, clientId);
    
    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "sensors_data");
    
         // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *dbb;

    public:
        callback(mongoc_database_t *dbb) : dbb(dbb) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');
            std::string machine_id = topic_parts[1];
            std::string sensor_id = topic_parts[2];

            std::string timestamp = j["timestamp"];
            float value = j["value"];

            if(teste_media > 7 && teste_media < 10){//teste para verificar o funcionamento
                value = 0;                          // do alarme de média (valor anormal)
            }
            
            teste_media++;

            std::cout << "Valor: " << value << std::endl;
            std::cout << "Data: " << timestamp << std::endl;


            if(media(value,machine_id,sensor_id)){
                 // Create the alarm message
                alarmes[sensor_id] = 1;
                
            }
            else{
                if(sensor_datas.size() > MAX_ELEMENTOS){
                    sensor_datas[sensor_id].pop_front();
                }
                sensor_datas[sensor_id].push_back(value);
            }

            last_data[sensor_id] = timestamp;
            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(dbb, sensor_id.c_str());
            insert_document(collection, machine_id, timestamp, value);
            mongoc_collection_destroy(collection);
        }
    };
    callback cb(database);
    client.set_callback(cb);
    

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    std::string topic;
    topic = "sensors/" + machine_id_parametro + "/" + sensor_id_parametro;
    
    try {
        client.connect(connOpts);
        client.subscribe(topic, QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        //return EXIT_FAILURE;
    }
    
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if(tempo_inativo(machine_id,sensor_id)){
            alarmes[sensor_id] = 2;
        }
        if(alarmes[sensor_id] > 0){
                nlohmann::json alarme;
                alarme["machine_id"] = machine_id;
                alarme["sensor_id"] = sensor_id;
                if(alarmes[sensor_id] == 1)
                    alarme["description"] = "Valor anormal detectado!";
                else
                    alarme["description"] = "Sensor inativo!";

                // Publish the JSON message to the appropriate topic
                std::string topic = "/alarmes";
                mqtt::message msg_alarme(topic, alarme.dump(), QOS, false);
                std::clog << "message published - topic: " << topic << " - message: " << alarme.dump() << std::endl;
                client.publish(msg_alarme);

        }
        alarmes[sensor_id] = 0;
    }
  
    //return EXIT_SUCCESS;

}


int main(int argc, char* argv[]) {
    std::string clientId = "clientId";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "sensors"); // replace with your database name



    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *db;

    public:
        callback(mongoc_database_t *db) : db(db) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());
            recebeu = true;


            std::string message = "mensagem_inicial";
            std::string machine_id = j["machine_id"];
            int intervalo = j["sensors"][ "data_interval"];
            std::string sensor_id = j["sensors"][ "sensor_id"];
            std::string  data_type = j["sensors"][ "data_type"];
            intervalo_sensor[sensor_id] = intervalo;

            aux_sensor_id = sensor_id;
            aux_machine_id = machine_id;
            aux_data = data_type;

            std::cout << "Data: " << intervalo << std::endl;
            std::cout << "Data: " << machine_id << std::endl;
        

            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(db, message.c_str());
            insert_document_initial(collection, machine_id, sensor_id, intervalo);
            mongoc_collection_destroy(collection);
        }
    };

    callback cb(database);
    client.set_callback(cb);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    std::string topic = "/sensor_monitors";

    try {
        client.connect(connOpts);
        client.subscribe(topic, QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if(alarme){
            threads.push_back(std::thread(alarmes_verified));
            alarme = false;  
        }
        if(recebeu){
            threads.push_back(std::thread(main_sensor, std::ref(aux_sensor_id), std::ref(aux_machine_id), std::ref(aux_data)));
            recebeu = false;
        }
    }

        for (auto& thread : threads) {
        thread.join();
    }

    // Cleanup MongoDB resources
    mongoc_database_destroy(database);
    mongoc_client_destroy(mongodb_client);
    mongoc_cleanup();

    return EXIT_SUCCESS;
}
