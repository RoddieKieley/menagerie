/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source.hpp>
#include <proton/source_options.hpp>

#include <iostream>
#include <string>

// The identity of the subscriber is the combination of container id
// and link (i.e. receiver) name
//
// - Set container id
// - Create receiver with link name, topic as source address, source is durable, and expiry_policy never
// - Use receiver.detach(), then close the connection

struct subscription_handler : public proton::messaging_handler {
    std::string conn_url_ {};
    std::string address_ {};
    int count_ {0};

    int received_ {0};
    bool stopping_ {false};

    void on_container_start(proton::container& cont) override {
        cont.connect(conn_url_);
    }

    void on_connection_open(proton::connection& conn) override {
        proton::receiver_options opts {};
        proton::source_options sopts {};

        opts.name("sub0"); // A stable name
        sopts.durability_mode(proton::source::UNSETTLED_STATE);
        sopts.expiry_policy(proton::source::NEVER);
        opts.source(sopts);
        
        conn.open_receiver(address_, opts);
    }

    void on_receiver_open(proton::receiver& rcv) override {
        std::cout << "RECEIVE: Opened receiver for source address '" << address_ << "'\n";
    }

    void on_message(proton::delivery& dlv, proton::message& msg) override {
        if (stopping_) return;

        std::cout << "RECEIVE: Received message '" << msg.body() << "'" << std::endl;

        received_++;

        if (received_ == count_) {
            dlv.receiver().detach(); // Detaching leaves the subscription intact (unclosed)
            dlv.connection().close();
            stopping_ = true;
        }
    }
};

int main(int argc, char** argv) {
    if (argc != 3 && argc != 4) {
        std::cerr << "Usage: receive CONNECTION-URL ADDRESS [COUNT]\n";
        return 1;
    }

    subscription_handler handler {};
    handler.conn_url_ = argv[1];
    handler.address_ = argv[2];

    if (argc == 4) {
        handler.count_ = std::stoi(argv[3]);
    }

    proton::container cont {handler, "example-app-1"};

    try {
        cont.run();
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    return 0;
}
