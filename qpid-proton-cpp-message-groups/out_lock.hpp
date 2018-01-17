//
//  out_lock.h
//  menagerie
//
//  Created by Roddie Kieley on 2018-01-10.
//
//

#ifndef out_lock_h
#define out_lock_h

#include <mutex>


// Lock output from threads to avoid scrambling
static std::mutex out_lock;
#define OUT(x) do { std::lock_guard<std::mutex> l(out_lock); x; } while (false)

#endif /* out_lock_h */
