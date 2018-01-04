//
//  receiver.cpp
//  menagerie
//
//  Created by Roddie Kieley on 2018-01-04.
//
//

#include "receiver.hpp"

#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/source.hpp>
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/receiver.hpp>

#include <iostream>


void receive_messaging_handler::
set_filter(proton::source_options& sopts, const std::string& selector_str)
{
    proton::source::filter_map sfm;
    proton::symbol filter_key("selector");
    proton::value filter_value;
    
    // The value is a specific AMQP "described type": string with symbolic descriptor
    proton::codec::encoder enc(filter_value);
    enc << proton::codec::start::described()
        << proton::symbol("apache.org:selector-filter:string")
        << selector_str
        << proton::codec::finish();
    
    // In our case the map has one element
    sfm.put(filter_key, filter_value);
    sopts.filters(sfm);
}

// Constructor(s)
receive_messaging_handler::
receive_messaging_handler(const std::string& url, const std::string& address, const std::string& group_str) :
url_(url),
address_(address),
group_str_(group_str)
{
    
}

// Destructor
receive_messaging_handler::
~receive_messaging_handler()
{
    
}

// proton::messaging_handler implementation
void receive_messaging_handler::
on_container_start(proton::container& cont)
{
    std::cout << group_str_ << " receive_messaging_handler::on_container_start" << std::endl;
    cont.connect(url_);
}

void receive_messaging_handler::
on_connection_open(proton::connection& conn)
{
    using namespace proton;
    
    std::cout << group_str_ << " receive_messaging_handler::on_connection_open" << std::endl;
    
    source_options sopts;
    receiver_options ropts;
    
    std::string filter_str = "group = '" + group_str_ + "'";
    set_filter(sopts, filter_str);
    conn.open_receiver(address_, ropts.source(sopts));
}

void receive_messaging_handler::
on_receiver_open(proton::receiver& rcv)
{
    std::cout << group_str_ << " receive_messaging_handler::on_receiver_open for address: " << address_ << std::endl;
}

void receive_messaging_handler::
on_message(proton::delivery& dlv, proton::message& msg)
{
    std::cout << group_str_ << " receive_messaging_handler::on_message msg: " << std::endl;
    std::cout << msg.body() << std::endl;
}

