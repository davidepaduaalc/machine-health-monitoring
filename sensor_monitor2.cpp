#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include <sys/statvfs.h>
#include <fstream>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"


float get_disk(){
    const char* mountPoint = "/"; // Diretório raiz

    struct statvfs stat;
    if (statvfs(mountPoint, &stat) == 0) {
        unsigned long long totalSpace = stat.f_blocks * stat.f_frsize;
        unsigned long long freeSpace = stat.f_bfree * stat.f_frsize;
        unsigned long long usedSpace = totalSpace - freeSpace;

        return static_cast<double>(usedSpace);
    } else {
        return 1;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    std::string clientId = "sensor-monitor";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "connected to the broker" << std::endl;

    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    std::string machineId(hostname);

    nlohmann::json j_initial;
    j_initial["machine_id"] = machineId;
    j_initial["sensors"][ "sensor_id"] = "0x355";
    j_initial["sensors"][ "data_type"] = "float";
    j_initial["sensors"][ "data_interval"] = 4;

    // Publish the JSON message to the appropriate topic.
    std::string topic = "/sensor_monitors";
    mqtt::message msg(topic, j_initial.dump(), QOS, false);
    std::clog << "message published - topic: " << topic << " - message: " << j_initial.dump() << std::endl;
    client.publish(msg);

    while (true) {
       // Get the current time in ISO 8601 format.
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Generate a random value.

        float value = get_disk();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        std::string topic = "/sensors/" + machineId + "/memory";
        mqtt::message msg(topic, j.dump(), QOS, false);
        std::clog << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;
        client.publish(msg);

        // Sleep for some time.
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}
