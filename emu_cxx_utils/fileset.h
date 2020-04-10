#pragma once

#include <cstdio>
#include <sstream>
#include <string>
#include "striped_array.h"
#include "replicated.h"
#include "repl_array.h"
#include "for_each.h"

// Logging macro. Flush right away since Emu hardware usually doesn't
#ifndef LOG
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);
#endif

// TODO add these to emu_c_utils
extern "C" {
#ifndef __le64__
inline FILE *
mw_fopen(const char *path, const char *mode, void *local_ptr) {
    (void) local_ptr;
    return fopen(path, mode);
}
inline int
mw_fclose(FILE *fp) {
    return fclose(fp);
}
inline size_t
mw_fread(void *ptr, size_t size, size_t nmemb, FILE *fp) {
    return fread(ptr, size, nmemb, fp);
}
inline size_t
mw_fwrite(void *ptr, size_t size, size_t nmemb, FILE *fp) {
    return fwrite(ptr, size, nmemb, fp);
}

#else
#include <memoryweb/io.h>
#endif
}

namespace emu {

class fileset {
private:
    // Open a file for each nodelet
    striped_array<FILE*> files_;
public:
    // Allow access to any file handle like an array
    FILE* operator[](long nlet) { return files_[nlet]; }

    explicit fileset(const char* basename, const char* mode)
    : files_(NODELETS())
    {
        const long num_nlets = NODELETS();
        for (long nlet = 0; nlet < num_nlets; ++nlet) {
            // Append suffix to each file: <nlet>of<nlets>
            std::ostringstream oss;
            oss << basename << "." << nlet << "of" << num_nlets;
            std::string slice_filename = oss.str();
            // Open the file
            FILE* fp = mw_fopen(slice_filename.c_str(), mode, &files_[nlet]);
            if (fp == nullptr) {
                LOG("Failed to open %s\n", slice_filename.c_str());
                exit(1);
            }
            // Store the handle
            files_[nlet] = fp;
        }
    }

    ~fileset()
    {
        // Close all the files
        const long num_nlets = NODELETS();
        for (long nlet = 0; nlet < num_nlets; ++nlet) {
            mw_fclose(files_[nlet]);
        }
    }
};

/**
 * Serialize a repl<T> to a fileset
 */
template<class T>
void serialize(fileset& f, repl<T>& item)
{
    // Write the nth copy to the nth file
    const long num_nlets = NODELETS();
    for (long nlet = 0; nlet < num_nlets; ++nlet) {
        size_t n = mw_fwrite(&item.get_nth(nlet), sizeof(T), 1, f[nlet]);
        if (n != 1) {
            LOG("Failed to write %lu bytes to file on nlet[%li]\n",
                sizeof(T), nlet);
            exit(1);
        }
    }
}

// Deserialize a repl<T> from a fileset
template<class T>
void deserialize(fileset& f, repl<T>& item)
{
    // Read the nth copy to the nth file
    const long num_nlets = NODELETS();
    for (long nlet = 0; nlet < num_nlets; ++nlet) {
        size_t n = mw_fread(&item.get_nth(nlet), sizeof(T), 1, f[nlet]);
        if (n != 1) {
            LOG("Failed to read %lu bytes from file on nlet[%li]\n",
                sizeof(T), nlet);
            exit(1);
        }
    }
}

// Serialize a striped_array<T> to a fileset
template<class T>
void serialize(fileset& f, striped_array<T>& array)
{
    // Spawn a thread for each nodelet
    const long num_nlets = NODELETS();
    emu::parallel::for_each(emu::parallel_policy<1>(),
        array.begin(), array.begin() + num_nlets,
        [&](T& stripe_first) {
            // Get the file associated with this nodelet
            long nlet = &stripe_first - array.begin();
            FILE *fp = f[nlet];

            // Save the size of the array to all slices
            long length = array.size();
            //LOG("nlet[%li]: Writing length = %li\n", nlet, length);
            mw_fwrite(&length, sizeof(long), 1, fp);

            // Get a pointer to the local stripe
            long *stripe = emu::pmanip::view2to1(&stripe_first);
            // Compute length of local stripe
            size_t stripe_len = array.size() / num_nlets;
            if (nlet < array.size() % num_nlets) { stripe_len += 1; }
            //LOG("nlet[%li]: Writing %li items\n", nlet, stripe_len);
            // Write the stripe to the file
            size_t n = mw_fwrite(stripe, sizeof(T), stripe_len, fp);
            if (n != stripe_len) {
                LOG("Failed to write %lu bytes to file on nlet[%li]\n",
                    n * sizeof(T), nlet);
                exit(1);
            }
        }
    );
}

// Deserialize a striped_array<T> from a fileset
// Contents of the array will be overwritten
template<class T>
void deserialize(fileset& f, striped_array<T>& array)
{
    const long num_nlets = NODELETS();

    // Read size of array from all slices
    auto length = emu::make_repl<long>();
    deserialize(f, *length);

    //LOG("Array length is %li\n", length->get());
    // Resize array
    array.resize(*length);

    // Spawn a thread for each nodelet
    emu::parallel::for_each(emu::parallel_policy<1>(),
        array.begin(), array.begin() + num_nlets,
        [&](T& stripe_first) {
            // Get the file associated with this nodelet
            long nlet = &stripe_first - array.begin();
            FILE *fp = f[nlet];

            // Get a pointer to the local stripe
            long *stripe = emu::pmanip::view2to1(&stripe_first);
            // Compute length of local stripe
            size_t stripe_len = array.size() / num_nlets;
            if (nlet < array.size() % num_nlets) { stripe_len += 1; }
            //LOG("nlet[%li]: Reading %li items\n", nlet, stripe_len);
            // Read the stripe from the file
            size_t n = mw_fread(stripe, sizeof(T), stripe_len, fp);
            if (n != stripe_len) {
                LOG("Failed to read %lu bytes from file on nlet[%li]\n",
                    n * sizeof(T), nlet);
                exit(1);
            }
        }
    );
}

// Serialize a repl_array<T> to a fileset
template<class T>
void serialize(fileset& f, repl_array<T>& array)
{
    // Spawn a thread for each nodelet
    const long num_nlets = NODELETS();
    for (long nlet = 0; nlet < num_nlets; ++ nlet) {
        // Get the file associated with this nodelet
        FILE *fp = f[nlet];
        // Save the size of the array to all slices
        long length = array.size();
        mw_fwrite(&length, sizeof(long), 1, fp);
        // Get a pointer to the local stripe
        long *stripe = array.get_nth(nlet);
        // Compute length of local stripe
        size_t stripe_len = array.size();
        // Write the stripe to the file
        size_t n = mw_fwrite(stripe, sizeof(T), stripe_len, fp);
        if (n != stripe_len) {
            LOG("Failed to write %lu bytes to file on nlet[%li]\n",
                n * sizeof(T), nlet);
            exit(1);
        }
    }
}

// Serialize a repl_array<T> to a fileset
template<class T>
void deserialize(fileset& f, repl_array<T>& array)
{
    // Spawn a thread for each nodelet
    const long num_nlets = NODELETS();
    // Read size of array from all slices
    auto length = emu::make_repl<long>();
    deserialize(f, *length);

    // Resize array
    array.resize(*length);

    for (long nlet = 0; nlet < num_nlets; ++ nlet) {
        // Get the file associated with this nodelet
        FILE *fp = f[nlet];
        // Read the  of the array to all slices
        long length = array.size();
        mw_fwrite(&length, sizeof(long), 1, fp);
        // Get a pointer to the local stripe
        long *stripe = array.get_nth(nlet);
        // Compute length of local stripe
        size_t stripe_len = array.size();
        // Write the stripe to the file
        size_t n = mw_fwrite(stripe, sizeof(T), stripe_len, fp);
        if (n != stripe_len) {
            LOG("Failed to write %lu bytes to file on nlet[%li]\n",
                n * sizeof(T), nlet);
            exit(1);
        }
    }
}


} // end namespace emu