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


//struct subscription_handler : public proton::messaging_handler {
//    std::string conn_url_ {};
//    std::string address_ {};
//    int count_ {0};
//    
//    int received_ {0};
//    bool stopping_ {false};
//    
//    void on_container_start(proton::container& cont) override {
//        cont.connect(conn_url_);
//    }
//    
//    void on_connection_open(proton::connection& conn) override {
//        proton::receiver_options opts {};
//        proton::source_options sopts {};
//        
//        //opts.name("sub0"); // A stable name
//        //sopts.durability_mode(proton::source::UNSETTLED_STATE);
//        //sopts.expiry_policy(proton::source::NEVER);
//        opts.source(sopts);
//        
//        conn.open_receiver(address_, opts);
//    }
//    
//    void on_receiver_open(proton::receiver& rcv) override {
//        std::cout << "RECEIVE: Opened receiver for source address '" << address_ << "'\n";
//    }
//    
//    void on_message(proton::delivery& dlv, proton::message& msg) override {
//        if (stopping_) return;
//        
//        std::cout << "RECEIVE: Received message '" << msg.body() << "'" << std::endl;
//        
//        received_++;
//        
//        if (received_ == count_) {
//            dlv.receiver().detach(); // Detaching leaves the subscription intact (unclosed)
//            dlv.connection().close();
//            stopping_ = true;
//        }
//    }
//};
//
//
//int main(int argc, char** argv) {
//    if (argc != 3 && argc != 4 && argc != 5) {
//        std::cerr << "Usage: " << argv[0] << "CONNECTION-URL ADDRESS [COUNT-groupA] [COUNT-groupB]\n";
//        return 1;
//    }
//
//    subscription_handler handler {};
//    handler.conn_url_ = argv[1];
//    handler.address_ = argv[2];
//
//    if (argc == 4) {
//        handler.count_ = std::stoi(argv[3]);
//    }
//
//    proton::container cont {handler, "example-app-1"};
//
//    try {
//        cont.run();
//    } catch (const std::exception& e) {
//        std::cerr << e.what() << std::endl;
//        return 1;
//    }
//
//    return 0;
//}

class send_messaging_handler :
    public proton::messaging_handler
{
private:
    const std::string       url_;
    const std::string       address_;
    
    proton::connection      connection_;
    proton::sender          sender_;
    
public:
    // Constructor(s)
    send_messaging_handler(const std::string& url, const std::string& address) :
        url_(url),
        address_(address)
    {
    }
    
    // Destructor
    virtual ~send_messaging_handler()
    {
        close();
    }
    
    // Method(s)
    void send(proton::message msg)
    {
        sender_.send(msg);
    }
    
    void close()
    {
        sender_.connection().close();
    }
    
    // proton::messaging_handler implementation
    void on_container_start(proton::container& cont) override
    {
        std::cout << "on_container_start" << std::endl;
        cont.connect(url_);
    }
    
    void on_connection_open(proton::connection& conn) override
    {
        std::cout << "on_connection_open" << std::endl;
        conn.open_sender(address_);
    }
    
    void on_sender_open(proton::sender& s) override
    {
        std::cout << "on_sender_open" << std::endl;
        sender_ = s;
        proton::message msg2(std::to_string(2));
        msg2.group_id("groupA");
        msg2.group_sequence(0);
        msg2.reply_to_group_id("replyGroup");

        proton::message msg3(std::to_string(3));
        msg3.group_id("groupB");
        msg3.group_sequence(0);
        msg3.reply_to_group_id("replyGroup");

        proton::message msg4(std::to_string(4));
        msg4.group_id("groupB");
        msg4.group_sequence(1);
        msg4.reply_to_group_id("replyGroup");
        
        s.send(msg2);
        s.send(msg3);
        s.send(msg4);
        
        s.link::close();
        s.session().close();
        s.connection().close();
    }
    
    void on_error(const proton::error_condition& e) override
    {
        std::cerr << "unexpected error: " << e << std::endl;
        exit(1);
    }
};

class receive_messaging_handler :
    public proton::messaging_handler
{
private:
    const std::string&      url_;
    const std::string&      address_;
    
public:
    // Constructor(s)
    receive_messaging_handler(const std::string& url, const std::string& address) :
        url_(url),
        address_(address)
    {
        
    }
    
    // Destructor
    virtual ~receive_messaging_handler()
    {
        
    }
    
    // proton::messaging_handler implementation
    void on_container_start(proton::container& cont) override
    {
        cont.connect(url_);
    }
    
    void on_connection_open(proton::connection& conn) override
    {
        using namespace proton;
        
        source_options sopts;
        receiver_options ropts;
        
        source::filter_map sfm;
        symbol k = "group-id";
        value v = "groupB";
        sfm.put(k,v);
        
        sopts.filters(sfm);
        ropts.source(sopts);
        
        conn.open_receiver(address_, ropts);
    }
    
    void on_receiver_open(proton::receiver& rcv) override
    {
        std::cout << "on_receiver_open for address: " << address_ << std::endl;
    }
    
    void on_message(proton::delivery& dlv, proton::message& msg) override
    {
        std::cout << "on_message" << std::endl;
        std::cout << "msg: " << msg.body() << std::endl;
    }
};

int main (int argc, char** argv)
{
    if (argc < 4)
    {
        //std::cerr << "Usage: " << argv[0] << "CONNECTION-URL ADDRESS [COUNT-groupA] [COUNT-groupB]"
        //return 1;
    }
    
    const char* url = "amqp://192.168.2.208";//argv[1];
    const char* address = "groupAddress";//argv[2];
    int n_messages_groupA = 10;//atoi(argv[3]);
    int n_messages_groupB = 10;//atoi(argv[4]);
    
    send_messaging_handler send_handler(url, address);
    receive_messaging_handler receive_handler(url, address);
    proton::container send_container(send_handler);
    proton::container receive_container(receive_handler);
    
    std::cout << "before send_container run" << std::endl;
    send_container.auto_stop(true);
    send_container.run();
    std::cout << "after send_container run" << std::endl;

    std::cout << "before receive_container run" << std::endl;
    receive_container.auto_stop(true);
    receive_container.run();
    std::cout << "after receive_container run" << std::endl;
    
//    for (int i = 0; i < n_messages_groupA; ++i)
//    {
//        std::cout << "creating msg: " << i << std::endl;
//        proton::message msg(std::to_string(i + 1));
//        std::cout << "sending msg: " << i << std::endl;
//        send_handler.send(msg);
//        std::cout << "sent msg: " << i << std::endl;
//    }
}