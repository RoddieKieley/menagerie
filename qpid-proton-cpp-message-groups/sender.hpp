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

#ifndef sender_hpp
#define sender_hpp

#include <proton/messaging_handler.hpp>
#include <proton/container.hpp>
#include <proton/sender.hpp>
#include <proton/receiver_options.hpp>
#include <proton/message.hpp>
#include <proton/delivery.hpp>

#include <condition_variable>
#include <mutex>
#include <string>
#include <queue>

// Forward declaration(s)
namespace proton
{
    class work_queue;
}


// A thread-safe sending connection that blocks sending threads when there
// is no AMQP credit to send messages.
class sender :
    private proton::messaging_handler
{
    // Only used in proton handler thread
    proton::sender sender_;
    
    // Shared by proton and user threads, protected by lock_
    std::mutex lock_;
    proton::work_queue* work_queue_;
    std::condition_variable sender_ready_;
    int queued_;                       // Queued messages waiting to be sent
    int credit_;                       // AMQP credit - number of messages we can send
    
public:
    sender(proton::container& cont, const std::string& url, const std::string& address);
    
    // Thread safe
    void send(const proton::message& m);
    void send(std::queue<proton::message>& messages);
    
    // Thread safe
    void close();
    
private:
    proton::work_queue* work_queue();
    
    // == messaging_handler overrides, only called in proton handler thread
    void on_sender_open(proton::sender& s) override;
    void on_sendable(proton::sender& s) override;
    void on_error(const proton::error_condition& e) override;
    
    // work_queue work items is are automatically dequeued and called by proton
    // This function is called because it was queued by send()
    void do_send(const proton::message& m);
};

#endif /* sender_hpp */
