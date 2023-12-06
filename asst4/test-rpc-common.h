// -*- c++ -*-
#ifndef TEST_RPC_COMMON_H
#define TEST_RPC_COMMON_H

#include "rpcxx.h"
#include <cassert>
#include <thread>
#include <ctime>
#include <cstdlib>

class ServiceTestUtil {
protected:
    rpc::Server *srv = nullptr;
    rpc::Client *client = nullptr;
    std::thread t;
    unsigned port;

public:
    ServiceTestUtil() {
        const unsigned min = 1024;
        const unsigned range = 65353 - min;
        srand(time(NULL));
        port = (rand() % range) + min;
    }

    ~ServiceTestUtil() {
        assert(srv == nullptr);
        assert(client == nullptr);
    }

    void SetUpServer() {
        srv = new rpc::Server();
        srv->Listen("127.0.0.1", (unsigned short)port);

        t = std::thread([this]() {
            srv->MainLoop();
        });
    }
    
    void SetUpClient() {
        client = new rpc::Client();
        client->Connect("127.0.0.1", (unsigned short)port);
    }
    
    void TearDownServer() {
        srv->SignalStop();
        t.join();
        delete srv;
        srv = nullptr;
    }
    
    void TearDownClient() {
        delete client;
        client = nullptr;
    }
};

#endif /* TEST_RPC_COMMON_H */
