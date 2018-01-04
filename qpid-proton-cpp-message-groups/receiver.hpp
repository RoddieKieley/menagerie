//
//  receiver.hpp
//  menagerie
//
//  Created by Roddie Kieley on 2018-01-04.
//
//

#ifndef receiver_hpp
#define receiver_hpp

#include <proton/messaging_handler.hpp>

#include <string>

// Forward declaration(s)
namespace proton
{
    class source_options;
    class container;
    class connection;
    class receiver;
    class delivery;
    class message;
}


class receive_messaging_handler :
    public proton::messaging_handler
{
private:
    const std::string&      url_;
    const std::string&      address_;
    const std::string&      group_str_;
    
    void set_filter(proton::source_options& sopts, const std::string& selector_str);
    
public:
    // Constructor(s)
    receive_messaging_handler(const std::string& url, const std::string& address, const std::string& group_str = "green");
    
    // Destructor
    virtual ~receive_messaging_handler();
    
    // proton::messaging_handler implementation
    void on_container_start(proton::container& cont) override;
    void on_connection_open(proton::connection& conn) override;
    void on_receiver_open(proton::receiver& rcv) override;
    void on_message(proton::delivery& dlv, proton::message& msg) override;
};

#endif /* receiver_hpp */
