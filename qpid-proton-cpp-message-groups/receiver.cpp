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

#include "receiver.hpp"
#include "out_lock.hpp"

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/work_queue.hpp>

#include <atomic>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>


receiver::receiver(proton::container& cont, const std::string& url, const std::string& address)
: work_queue_()
{
    // NOTE:credit_window(0) disables automatic flow control.
    // We will use flow control to match AMQP credit to buffer capacity.
    cont.open_receiver(url+"/"+address, proton::receiver_options::receiver_options().credit_window(0),
                       proton::connection_options::connection_options().handler(*this));
}

// Thread safe receive
bool receiver::receive(proton::message& m, unsigned int seconds_timeout) {
    std::unique_lock<std::mutex> l(lock_);
    
    bool message_received = false;
    std::chrono::high_resolution_clock hrc;
    auto tp_now = hrc.now();
    auto tp_end = tp_now + std::chrono::seconds(30);
    
    // Wait for buffered messages
    while (!work_queue_ || buffer_.empty())
    {
        if (0 == seconds_timeout)
        {
            can_receive_.wait(l);
        }
        else
        {
            can_receive_.wait_until(l, tp_end);
            OUT(std::cout << "receiver::receive() wait time of " << seconds_timeout << " up" << std::endl);
            break;
        }
    }
    
    if (!buffer_.empty())
    {
        m = std::move(buffer_.front());
        buffer_.pop();
        // Add a lambda to the work queue to call receive_done().
        // This will tell the handler to add more credit.
        work_queue_->add([=]() { this->receive_done(); });
        message_received = true;
    }

    return message_received;
}

void receiver::close() {
    std::lock_guard<std::mutex> l(lock_);
    if (work_queue_) work_queue_->add([this]() { this->receiver_.connection().close(); });
}

// ==== The following are called by proton threads only.
void receiver::on_receiver_open(proton::receiver& r) {
    receiver_ = r;
    std::lock_guard<std::mutex> l(lock_);
    work_queue_ = &receiver_.work_queue();
    receiver_.add_credit(MAX_BUFFER); // Buffer is empty, initial credit is the limit
}

void receiver::on_message(proton::delivery &d, proton::message &m) {
    // Proton automatically reduces credit by 1 before calling on_message
    std::lock_guard<std::mutex> l(lock_);
    buffer_.push(m);
    can_receive_.notify_all();
}

// called via work_queue
void receiver::receive_done() {
    // Add 1 credit, a receiver has taken a message out of the buffer.
    receiver_.add_credit(1);
}

void receiver::on_error(const proton::error_condition& e) {
    OUT(std::cerr << "unexpected error: " << e << std::endl);
    exit(1);
}
