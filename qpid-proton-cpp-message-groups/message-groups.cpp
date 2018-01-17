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

#include "message-groups.hpp"
#include "receiver.hpp"
#include "sender.hpp"
#include "out_lock.hpp"

#include <proton/container.hpp>
#include <proton/message.hpp>

#include <iostream>
#include <sstream>
#include <thread>
#include <string>
#include <atomic>
#include <chrono>
#include <vector>
#include <queue>
#include <algorithm>
#include <assert.h>


// ==== Example code using the sender and receiver
void generate_messages(std::queue<proton::message>& messages, int n, std::string& group_id)
{
    for (int i = 0; i < n; ++i)
    {
        std::ostringstream ssid;
        std::ostringstream ss;
        ss << std::this_thread::get_id() << "-" << i;
        proton::message m = proton::message(ss.str());
        
        m.group_id(group_id);
        if ((messages.size() == n-1) && (!group_id.empty()))
        {
            m.group_sequence(-1);
        }
        messages.push(m);
    }
}

// Send n messages
void send_thread(sender& s, int n, bool use_message_grouping = false)
{
    assert(n >= 4);
    
    std::queue<proton::message> messages;
    std::string group_id = "";
    auto id = std::this_thread::get_id();

    if (use_message_grouping)
        group_id = "groupA";
    
    generate_messages(messages, 8, group_id);
    s.send(messages);
    OUT(std::cout << id << " sent " << n << std::endl);
    
    if (use_message_grouping)
        group_id = "groupB";
    generate_messages(messages, 8, group_id);
    s.send(messages);
    OUT(std::cout << id << " sent " << n << std::endl);
}

// Receive messages till atomic remaining count is 0.
// remaining is shared among all receiving threads
void receive_thread(receiver& r, std::atomic_int& remaining, int thread_index)
{
    // atomically check and decrement remaining *before* receiving.
    // If it is 0 or less then return, as there are no more
    // messages to receive so calling r.receive() would block forever.
    int n = 0;
    proton::message m;
    bool message_received = false;
    do
    {
        message_received = r.receive(m);
        if (message_received)
            ++n;
        if (0 == thread_index)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        OUT(std::cout << "receiver" << thread_index << " received \"" << m.body() << '"' << " group-id " << m.group_id() << " group-sequence " << m.group_sequence() << " reply-to-group-id " << m.reply_to_group_id() << " remaining " << remaining << std::endl);
    }
    while (--remaining > 0);
    OUT(std::cout << "receiver" << thread_index << " received " << n << " messages" << std::endl);
}

void receive_thread(receiver& r, int thread_index, int seconds_timeout)
{
    int n = 0;
    proton::message m;
    bool message_received = false;

    do
    {
        message_received = r.receive(m, seconds_timeout);
        if (0 == thread_index)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        if (message_received)
        {
            ++n;
            OUT(std::cout << "receiver" << thread_index << " received \"" << m.body() << '"' << " group-id " << m.group_id() << " group-sequence " << m.group_sequence() << " reply-to-group-id " << m.reply_to_group_id() << std::endl);
        }
        else
        {
            break;
        }
    }
    while (true);
    OUT(std::cout << "receiver" << thread_index << " received " << n << " messages" << std::endl);
}


int main(int argc, const char **argv) {
    try {
        if (argc != 4) {
            std::cerr <<
            "Usage: " << argv[0] <<
            "CONNECTION-URL: connection address, e.g.'amqp://127.0.0.1'\n"
            "AMQP-ADDRESS: AMQP node address, e.g. 'examples'\n"
            "GROUPS: 1 to use message groups, 0 to not use message groups\n";
            return 1;
        }
        
        const int message_count = 16;
        const char *url = argv[1];
        const char *address = argv[2];
        const bool use_message_groups = atoi(argv[3]) > 0;
        std::vector<std::thread> threads;
        
        // Total messages to be received, multiple receiver threads will decrement this.
        std::atomic_int remaining0;
        std::atomic_int remaining1;
        if (use_message_groups)
        {
            remaining0.store(8);
            remaining1.store(8);
        }
        
        // Run the proton container
        proton::container container;
        auto container_thread = std::thread([&]() { container.run(); });
        
        sender send(container, url, address);
        receiver recv0(container, url, address);
        receiver recv1(container, url, address);

        OUT(std::cout << "Starting sending thread for 8 messages per group\n");
        OUT(std::cout << "Each thread individually waits 20 seconds maximum after the last message received if any\n");
        threads.push_back(std::thread([&]() { send_thread(send, message_count, use_message_groups); }));
        
        OUT(std::cout << "Sleeping for 2 seconds\n");
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        OUT(std::cout << "Starting receiver threads 0, 1\n");
        if (use_message_groups)
        {
            threads.push_back(std::thread([&]() { receive_thread(recv0, remaining0, 0); }));
            threads.push_back(std::thread([&]() { receive_thread(recv1, remaining1, 1); }));
        }
        else
        {
            threads.push_back(std::thread([&]() { receive_thread(recv0, 0, 20); }));
            threads.push_back(std::thread([&]() { receive_thread(recv1, 1, 20); }));
        }

        // Wait for threads to finish
        OUT(std::cout << "Waiting for threads to finish\n");
        for (auto& t : threads)
            t.join();
        send.close();
        recv0.close();
        recv1.close();
        container_thread.join();
        if ((remaining0 > 0) || (remaining1 > 0))
            throw std::runtime_error("not all messages were received");
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return 1;
}
