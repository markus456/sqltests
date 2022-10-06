#pragma once
#include <iostream>

bool debug_enabled();

void set_debug(bool value);

template <class... Args>
void debug(Args... args)
{
    if (debug_enabled())
    {
        ((std::cout << args << " "), ...) << std::endl;
    }
}
