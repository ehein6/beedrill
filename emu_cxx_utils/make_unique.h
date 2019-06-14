#pragma once
#include <memory>

//// std::make_unique added in C++14
//#if (__cplusplus < 201402L)
//// Definition of make_unique taken from https://herbsutter.com/gotw/_102/
//namespace std {
//template<typename T, typename ...Args>
//std::unique_ptr<T> make_unique( Args&& ...args )
//{
//    return std::unique_ptr<T>( new T( std::forward<Args>(args)... ) );
//}
//}
//#endif

