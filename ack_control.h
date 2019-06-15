#pragma once

#include <emu_c_utils/emu_c_utils.h>

// HACK until these macros are added to memoryweb_x86
#ifndef DISABLE_ACKS
#define DISABLE_ACKS()
#define ENABLE_ACKS()
#endif


class ack_controller
{
private:
    emu::repl_copy<emu::striped_array<volatile long>> data_;
    ack_controller() : data_(NODELETS()) {}
public:

    static ack_controller&
    instance()
    {
        // Singleton pattern, instance will be constructed on first use only
        static replicated ack_controller instance;
        return instance;
    }

    void disable_acks()
    {
        DISABLE_ACKS();
    }

    void
    reenable_acks()
    {
        // Re-enable ack's
        ENABLE_ACKS();
        // Do a remote write to each nodelet.
        // These will be queued behind all previous remotes
        for (long nlet = 0; nlet < NODELETS(); ++nlet) {
            data_[nlet] = 1;
        }
        // Wait for them to complete
        FENCE();
    }
};

