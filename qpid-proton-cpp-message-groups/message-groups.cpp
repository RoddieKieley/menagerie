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

#include "message-groups.h"
#include "receiver.hpp"

#include <proton/container.hpp>

#include <iostream>


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
