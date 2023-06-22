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


#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

std::map<std::string, std::deque<float>> sensor_datas;

std::map<std::string,std::string> last_data;

const int MAX_ELEMENTOS = 10;

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

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}


void media(float value, std::string machine_id, std::string sensor_id) {
    float sum = 0.0;
    int tam = 0;
    if (sensor_datas[sensor_id].size() < 6) {
        return;
    }
    for (const auto& data_value : sensor_datas[sensor_id]) {
        sum = sum + data_value;
        tam++;
    }
    float media = sum / tam;
    if (value < 0.8 * media && value > 1.2 * media) {
        // Create an MQTT client for publishing
        std::string brokerAddress = "tcp://localhost:1883";
        std::string clientId = "publisherClient";
        mqtt::client pubClient(brokerAddress, clientId);

        // Connect to the MQTT broker
        mqtt::connect_options connOpts;
        connOpts.set_keep_alive_interval(20);
        connOpts.set_clean_session(true);

        try {
            pubClient.connect(connOpts);
        } catch (mqtt::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return ;
        }

        // Create the alarm message
        nlohmann::json alarme;
        alarme["machine_id"] = machine_id;
        alarme["sensor_id"] = sensor_id;
        alarme["description"] = "Valor anormal detectado!";

        // Publish the JSON message to the appropriate topic
        std::string topic = "/alarmes";
        mqtt::message msg_alarme(topic, alarme.dump(), QOS, false);
        std::clog << "message published - topic: " << topic << " - message: " << alarme.dump() << std::endl;
        pubClient.publish(msg_alarme);

        // Disconnect from the MQTT broker
        pubClient.disconnect();

    }
    return;
}

void tempo_inativo(){





}

int main(int argc, char* argv[]) {
    std::string clientId = "clientId";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "sensors_data"); // replace with your database name

    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *db;
        mqtt::client& client;

    public:
        callback(mongoc_database_t *db,mqtt::client& client) : db(db), client(client) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');
            std::string machine_id = topic_parts[2];
            std::string sensor_id = topic_parts[3];


            std::string timestamp = j["timestamp"];
            float value = j["value"];
            
            media(value,machine_id,sensor_id);

            if(sensor_datas.size() > MAX_ELEMENTOS){
                sensor_datas[sensor_id].pop_front();
            }
            sensor_datas[sensor_id].push_back(value);

            
            std::cout << "Valor: " << sensor_datas[sensor_id][0] << std::endl;
            std::cout << "Data: " << timestamp << std::endl;
        

            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(db, sensor_id.c_str());
            insert_document(collection, machine_id, timestamp, value);
            mongoc_collection_destroy(collection);
        }
    };


    callback cb(database,client);
    client.set_callback(cb);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
        client.subscribe("/sensors/#", QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Cleanup MongoDB resources
    mongoc_database_destroy(database);
    mongoc_client_destroy(mongodb_client);
    mongoc_cleanup();

    return EXIT_SUCCESS;
}
