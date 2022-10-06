#include "diagnostics.hh"

static bool s_debug = false;

bool debug_enabled()
{
    return s_debug;
}

void set_debug(bool value)
{
    s_debug = value;
}