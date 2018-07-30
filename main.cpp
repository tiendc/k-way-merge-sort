//
// ExternalSort test
// Compile with -std=gnu++11 -lpthread
//

#include "external_sort.h"
#include <climits>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;


typedef std::string DataType;

// Data type comparator
bool stringCmpAsc(const DataType &a, const DataType &b)
{
    return a < b;
}


struct Args
{
    std::string input;
    std::string output;
    mem_size_t maxBuffSz;
    unsigned maxChunkInMerge;
    unsigned maxThreadInMerge;
    bool (*cmpFunc)(const DataType &a, const DataType &b);

    Args()
    {
        maxBuffSz = 0; // 0 means unlimited
        maxChunkInMerge = 0; // 0 means unlimited
        maxThreadInMerge = 0; // 0 means unlimited
        cmpFunc = stringCmpAsc;
    }
};


// Parse args from commandline
bool parseArgs(int argc, char *argv[], Args& args)
{
    if (argc < 3 || argc > 6)
    {
        return false;
    }
    else
    {
        args.input = argv[1];
        args.output = argv[2];
        if (argc >= 4)
        {
            const char* memArg = argv[3];
            char* endptr = NULL;
            args.maxBuffSz = strtoll(memArg, &endptr, 10) * 1024 * 1024;

            // Invalid input for maxBuffSz (eg. not a nunber)
            if (endptr && endptr != memArg + strlen(memArg))
                return false;
        }
        if (argc >= 5)
        {
            const char* maxChunkArg = argv[4];
            char* endptr = NULL;
            args.maxChunkInMerge = strtoul(maxChunkArg, &endptr, 10);

            // Invalid input for maxChunkInMerge (eg. not a nunber)
            if (endptr && endptr != maxChunkArg + strlen(maxChunkArg))
                return false;
        }
        if (argc >= 6)
        {
            const char* maxThreadArg = argv[5];
            char* endptr = NULL;
            args.maxThreadInMerge = strtoul(maxThreadArg, &endptr, 10);

            // Invalid input for maxThreadInMerge (eg. not a nunber)
            if (endptr && endptr != maxThreadArg + strlen(maxThreadArg))
                return false;
        }
        
        return true;
    }
}


int main(int argc, char *argv[])
{
    Args args;

    // Parse args
    if (!parseArgs(argc, argv, args))
    {
        cout << "Usage: ext_sort <input_file> <output_file> "
             << "[max_buff_sz_mb] [max_chunk_in_merge] [max_thread_in_merge]\n"
             << "Eg. ext_sort in.txt out.txt 10 5 7"
             << "  will do the sort with 10mb mem, max 5 chunks, 7 threads\n";
        return 1;
    }
    else
    {
        cout << "\nK-WAY MERGE SORT\n\n";

        cout << "Input Args:\n";
        cout << "   Input: " << args.input << "\n";
        cout << "   Output: " << args.output << "\n";
        cout << "   Max buffer size: " << args.maxBuffSz << " bytes\n";
        cout << "   Max chunk in merge: " << args.maxChunkInMerge << " chunks\n";
        cout << "   Max thread in merge: " << args.maxThreadInMerge << " threads\n";
    }

    // Open output file
    std::ofstream outFile;
    outFile.open(args.output.c_str(), ios::out);

    ExternalSort<DataType> sorter(args.input,
                                  &outFile, 
                                  args.cmpFunc,
                                  args.maxBuffSz,
                                  args.maxChunkInMerge,
                                  args.maxThreadInMerge);
    
    sorter.sort();

    // Close output file
    outFile.close();

    std::cout << "\nDONE.\n\n";
    return 0;
}
