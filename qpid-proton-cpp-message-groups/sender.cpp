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

#include "sender.hpp"
#include "out_lock.hpp"

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/work_queue.hpp>

#include <atomic>
#include <iostream>
#include <sstream>
#include <thread>


sender::sender(proton::container& cont, const std::string& url, const std::string& address)
: work_queue_(0), queued_(0), credit_(0)
{
    cont.open_sender(url+"/"+address, proton::connection_options().handler(*this));
}

// Thread safe
void sender::send(const proton::message& m) {
    {
        std::unique_lock<std::mutex> l(lock_);
        // Don't queue up more messages than we have credit for
        while (!work_queue_ || queued_ >= credit_) sender_ready_.wait(l);
        ++queued_;
    }
    work_queue_->add([=]() { this->do_send(m); }); // work_queue_ is thread safe
}

void sender::send(std::queue<proton::message>& messages)
{
    while(!messages.empty())
    {
        proton::message& m = messages.front();
        send(m);
        OUT(std::cout << std::this_thread::get_id() << " sent \"" << m.body() << '"' << " group_id " << m.group_id() << " group_sequence " << m.group_sequence() << std::endl);
        messages.pop();
    }
}

// Thread safe
void sender::close() {
    work_queue()->add([=]() { sender_.connection().close(); });
}

proton::work_queue* sender::work_queue() {
    // Wait till work_queue_ and sender_ are initialized.
    std::unique_lock<std::mutex> l(lock_);
    while (!work_queue_) sender_ready_.wait(l);
        return work_queue_;
}

// == messaging_handler overrides, only called in proton handler thread

void sender::on_sender_open(proton::sender& s) {
    // Make sure sender_ and work_queue_ are set atomically
    std::lock_guard<std::mutex> l(lock_);
    sender_ = s;
    work_queue_ = &s.work_queue();
}

void sender::on_sendable(proton::sender& s) {
    std::lock_guard<std::mutex> l(lock_);
    credit_ = s.credit();
    sender_ready_.notify_all(); // Notify senders we have credit
}

// work_queue work items is are automatically dequeued and called by proton
// This function is called because it was queued by send()
void sender::do_send(const proton::message& m) {
    sender_.send(m);
    std::lock_guard<std::mutex> l(lock_);
    --queued_;                    // work item was consumed from the work_queue
    credit_ = sender_.credit();   // update credit
    sender_ready_.notify_all();       // Notify senders we have space on queue
}

void sender::on_error(const proton::error_condition& e) {
    OUT(std::cerr << "unexpected error: " << e << std::endl);
    exit(1);
}