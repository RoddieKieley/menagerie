/*
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
 */

#ifndef receiver_hpp
#define receiver_hpp

#include <proton/messaging_handler.hpp>
#include <proton/container.hpp>
#include <proton/receiver.hpp>
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


// A thread safe receiving connection that blocks receiving threads when there
// are no messages available, and maintains a bounded buffer of incoming
// messages by issuing AMQP credit only when there is space in the buffer.
class receiver :
    private proton::messaging_handler
{
    static const size_t MAX_BUFFER = 100; // Max number of buffered messages
    
    // Used in proton threads only
    proton::receiver receiver_;
    
    // Used in proton and user threads, protected by lock_
    std::mutex lock_;
    proton::work_queue* work_queue_;
    std::queue<proton::message> buffer_; // Messages not yet returned by receive()
    std::condition_variable can_receive_; // Notify receivers of messages
    
public:
    
    // Connect to url
    receiver(proton::container& cont, const std::string& url, const std::string& address);
    
    // Thread safe receive
    bool receive(proton::message& m, unsigned int seconds_timeout = 0);
    
    void close();
    
private:
    // ==== The following are called by proton threads only.
    // ---- messaging_handler interface overrides
    void on_receiver_open(proton::receiver& r) override;
    void on_message(proton::delivery& d, proton::message& m) override;
    void on_error(const proton::error_condition& e) override;
    
    // called via work_queue
    void receive_done();
};

#endif /* receiver_hpp */
