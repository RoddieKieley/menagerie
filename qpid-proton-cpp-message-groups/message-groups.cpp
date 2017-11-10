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


class receive_messaging_handler :
    public proton::messaging_handler
{
private:
    const std::string&      url_;
    const std::string&      address_;
    const std::string&      group_str_;
    
    
    void set_filter(proton::source_options& sopts, const std::string& selector_str)
    {
        proton::source::filter_map sfm;
        proton::symbol filter_key("selector");
        proton::value filter_value;
        
        // The value is a specific AMQP "described type": binary string with symbolic descriptor
        proton::codec::encoder enc(filter_value);
        enc << proton::codec::start::described()
            << proton::symbol("apache.org:selector-filter:string")
            << proton::binary(selector_str) // needs to be string?
            << proton::codec::finish();
        
        // In our case the map has one element
        sfm.put(filter_key, filter_value);
        sopts.filters(sfm);
    }
    
public:
    // Constructor(s)
    receive_messaging_handler(const std::string& url, const std::string& address, const std::string& group_str = "green") :
        url_(url),
        address_(address),
        group_str_(group_str)
    {
        
    }
    
    // Destructor
    virtual ~receive_messaging_handler()
    {
        
    }
    
    // proton::messaging_handler implementation
    void on_container_start(proton::container& cont) override
    {
        std::cout << group_str_ << " receive_messaging_handler::on_container_start" << std::endl;
        cont.connect(url_);
    }
    
    void on_connection_open(proton::connection& conn) override
    {
        using namespace proton;
        
        std::cout << group_str_ << " receive_messaging_handler::on_connection_open" << std::endl;
        
        source_options sopts;
        receiver_options ropts;

        std::string filter_str = "group = '" + group_str_ + "'";
        set_filter(sopts, filter_str);
        conn.open_receiver(address_, ropts.source(sopts));
    }
    
    void on_receiver_open(proton::receiver& rcv) override
    {
        std::cout << group_str_ << " receive_messaging_handler::on_receiver_open for address: " << address_ << std::endl;
    }
    
    void on_message(proton::delivery& dlv, proton::message& msg) override
    {
        std::cout << group_str_ << " receive_messaging_handler::on_message msg: " << std::endl;
        std::cout << msg.body() << std::endl;
    }
};

int main (int argc, char** argv)
{
    if (argc > 4)
    {
        std::cerr << "Usage: " << argv[0] << " CONNECTION-URL ADDRESS GROUP-STRING" << std::endl;
        return 1;
    }
    
    const char* url = (argv[1] && argc > 1) ? argv[1] : "amqp://192.168.2.208";
    const char* address = (argv[2] && argc > 2) ? argv[2] : "groupAddress";
    const char* group = (argv[3] && argc > 3) ? argv[3] : "red";

    receive_messaging_handler group_receive_handler(url, address, group);
    proton::container group_receive_container(group_receive_handler);

    std::cout << "before group_receive_container run" << std::endl;
    group_receive_container.auto_stop(true);
    group_receive_container.run();
    std::cout << "after group_receive_container run" << std::endl;
}