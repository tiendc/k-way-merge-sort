#ifndef EXTERNAL_SORT_H_
#define EXTERNAL_SORT_H_


#define USE_THREAD 1
#define MEASURE_TIME 1

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

#if USE_THREAD
#include <condition_variable>
#include <mutex>
#include <thread>
#endif

using namespace std;


#if USE_THREAD
class MergePool
{
private:
    int _maxThreads;
    std::list<std::function<void()>> _queue;
    std::list<std::thread> _threads;

    std::mutex _mtx;
    std::condition_variable _cv;

public:
    MergePool(unsigned maxThreads = 5)
    : _maxThreads(maxThreads)
    {}

    unsigned threadCount()
    {
        return _threads.size();
    }

    unsigned queueCount()
    {
        return _queue.size();
    }

    void threadFunc(const std::function<void()>& func)
    {
        // Execute the target function
        func();

        // Remove the thread from the list
        {
            std::unique_lock<std::mutex> lock(_mtx);
            const auto& this_id = std::this_thread::get_id();
            for (auto it = _threads.begin(); it != _threads.end(); ++it)
            {
                if (it->get_id() == this_id)
                {
                    it->detach();
                    _threads.erase(it);
                    break;
                }
            }
            _cv.notify_all();
        }

        checkQueue();
    }

    void addWorker(const std::function<void()>& func)
    {
        {
            std::unique_lock<std::mutex> lock(_mtx);
            _queue.push_back(func);
        }
        checkQueue();
    }

    void waitAll()
    {
        std::unique_lock<std::mutex> lock(_mtx);
        while (!_threads.empty() || !_queue.empty())
            _cv.wait(lock);
    }

private:
    void checkQueue()
    {
        std::unique_lock<std::mutex> lock(_mtx);
        if (!_queue.empty() &&
            (_maxThreads == 0 || _maxThreads > _threads.size()))
        {
            auto func = _queue.front();
            _queue.pop_front();
            _threads.push_back(std::thread(
                std::bind(&MergePool::threadFunc, this, func)));
        }
    }
};
#endif


typedef unsigned long long mem_size_t;
typedef std::shared_ptr<std::ifstream> ifstream_ptr;


template <class T>
class ExternalSort
{
    typedef std::vector<T> DataBuffer;
    typedef std::vector<ifstream_ptr> ChunkFileList;
    
public:
    ExternalSort(const std::string &input,
                 std::ostream *output,
                 bool (*cmpFunc)(const T &a, const T &b) = NULL,
                 mem_size_t maxBuffSz = 1024 * 1024,
                 unsigned maxChunkInMerge = 0,
                 unsigned maxThreadInMerge = 0,
                 std::string tempDirPath = ".");

    ~ExternalSort();

    // Public API for doing the sort
    void sort();

protected:
    virtual unsigned getItemSize(const T& item);

private:
    std::string _input;
    std::ostream *_output;
    bool (*_cmpFunc)(const T &a, const T &b);
    mem_size_t _maxBuffSz;
    unsigned _maxChunkInMerge;
    std::string _tempDirPath;

#if USE_THREAD
    MergePool _mergePool;
#endif

    // Devide the input data into chunks and sort them
    std::vector<std::string> devideAndSort();
    void sortBuffer(DataBuffer& buffer);

    // Merge chunks by batch
    bool merge(const std::vector<std::string>& chunkNames,
               unsigned batchIndex = 0);
    // Merge chunks and write to output
    bool mergeChunks(const std::vector<std::string>& chunkNames,
                     const std::string& output,
                     unsigned startIndex);
    bool mergeChunks(const std::vector<std::string>& chunkNames,
                     std::ostream& output,
                     unsigned startIndex);

    // Read/Write chunks
    std::string writeChunk(const DataBuffer& data, unsigned chunkIndex);
    ChunkFileList openChunks(const std::vector<std::string>& chunkNames);
    void cleanupChunks(const std::vector<std::string>& chunkNames,
                       ChunkFileList& chunkFiles);

protected:
    class MergeItem
    {
    public:
        T data;
        ifstream_ptr stream;
        bool (*cmpFunc)(const T& a, const T& b);

        MergeItem(const T& data,
                  ifstream_ptr stream,
                  bool (*cmpFunc)(const T &a, const T &b))
        :
            data(data),
            stream(stream),
            cmpFunc(cmpFunc)
        {}

        bool operator< (const MergeItem& a) const
        {
            return !(cmpFunc(data, a.data));
        }
    };

    friend class MergeItem;
};


template <class T>
ExternalSort<T>::ExternalSort (const std::string &input,
                               std::ostream *output,
                               bool (*cmpFunc)(const T &a, const T &b),
                               mem_size_t maxBuffSz,
                               unsigned maxChunkInMerge,
                               unsigned maxThreadInMerge,
                               std::string tempDirPath)
    : _input(input)
    , _output(output)
    , _cmpFunc(cmpFunc)
    , _maxBuffSz(maxBuffSz)
    , _maxChunkInMerge(maxChunkInMerge)
#if USE_THREAD
    , _mergePool(maxThreadInMerge)
#endif
    , _tempDirPath(tempDirPath)
{}

template <class T>
ExternalSort<T>::~ExternalSort()
{}

template <class T>
void ExternalSort<T>::sort()
{
#if MEASURE_TIME
    auto t0 = std::chrono::high_resolution_clock::now();
#endif

    const auto& chunkNames = devideAndSort();
    merge(chunkNames);

#if MEASURE_TIME
    auto t1 = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
    std::cout << "\nDuration: " << ms.count() / 1000.0 << "s\n";
#endif
}

template <class T>
unsigned ExternalSort<T>::getItemSize(const T& item)
{
    return sizeof(item);
}

// Specialization for std::string item type
template <>
unsigned ExternalSort<std::string>::getItemSize(const std::string& item)
{
    return item.size();
}

template <class T>
std::vector<std::string> ExternalSort<T>::devideAndSort()
{
    std::vector<std::string> chunkNames;
    
    std::ifstream input;
    input.open(_input.c_str(), ios::in);

    if (!input.good())
    {
        std::cerr << "Error: Unable to open input file "
            << _input << std::endl;
        return chunkNames;
    }

    std::cout << "\nDEVIDE AND SORT\n";

    DataBuffer buffer;
    mem_size_t totalBytes = 0;
    mem_size_t estimatedBuffSz = _maxBuffSz / 10 * 9; // 90% of max size
    buffer.reserve(1000000);

    // Read input data
    T item;
    while (input >> item)
    {
        buffer.push_back(item);
        totalBytes += getItemSize(item);

        // Sort the buffer and write a chunk if we reach the quota
        if (estimatedBuffSz > 0 && totalBytes > estimatedBuffSz)
        {
            sortBuffer(buffer);

            // Write the sorted data to a chunk file
            const auto& name = writeChunk(buffer, chunkNames.size());
            chunkNames.push_back(name);

            // Clear the buffer for the next run
            buffer.clear();
            totalBytes = 0;
        }
    }

    if (!buffer.empty())
    {
        // Process the last chunk
        if (!chunkNames.empty())
        {
            sortBuffer(buffer);

            // Write the last chunk
            const auto& name = writeChunk(buffer, chunkNames.size());
            chunkNames.push_back(name);
        }
        else
        {
            std::cout << "\nEntire input data fit in memory\n";

            // Entire input file is fit into memory
            sortBuffer(buffer);

            // Write the sorted data
            for (size_t i = 0; i < buffer.size(); ++i)
                *_output << buffer[i] << std::endl;
        }
    }

    return chunkNames;
}

template <class T>
void ExternalSort<T>::sortBuffer(DataBuffer& buffer)
{
    if (_cmpFunc)
        std::sort(buffer.begin(), buffer.end(), *_cmpFunc);
    else
        std::sort(buffer.begin(), buffer.end());
}

template <class T>
bool ExternalSort<T>::merge(const std::vector<std::string>& chunkNames,
                            unsigned batchIndex)
{
    // No chunks to merge
    if (chunkNames.empty())
        return false;

    unsigned maxChunk = _maxChunkInMerge;
    if (maxChunk > 0 && chunkNames.size() > maxChunk)
    {
        std::cout << "\n\nMERGE CHUNKS (batch " << batchIndex << ")\n";

        std::vector<std::string> newChunkNames;

        // Merge up to _maxChunkInMerge chunks in a batch
        unsigned start = 0;
        unsigned chunkIndex = 0;
        bool success = true;
        while (start < chunkNames.size())
        {
            unsigned end = std::min((size_t) start + maxChunk,
                                    chunkNames.size());
            std::vector<std::string> subChunks(chunkNames.begin() + start,
                                               chunkNames.begin() + end);

            std::ostringstream oss;
            oss << _tempDirPath << "/" << _input
                << ".batch_" << batchIndex
                << ".chunk_" << chunkIndex;
    
            std::string chunkName = oss.str();
            newChunkNames.push_back(chunkName);
    
            auto func = [this, subChunks, chunkName, start]()
            {
                return this->mergeChunks(subChunks, chunkName, start);
            };

#if USE_THREAD
            _mergePool.addWorker(func);
#else
            success = func();
#endif

            start += maxChunk;
            chunkIndex++;
        }

#if USE_THREAD
        _mergePool.waitAll();
#endif

        if (success)
            return merge(newChunkNames, batchIndex + 1);
        else
        {
            std::cout << "\nFailed to merge batch " << batchIndex
                << "(chunk " << start << "-" << start + maxChunk << ")\n";
            return false;
        }
    }
    else
    {
        std::cout << "\n\nMERGE CHUNKS"
            << (batchIndex > 0 ? " (last batch)\n" : "\n");

        // Merge all chunks
        return mergeChunks(chunkNames, *_output, 0);
    }
}

template <class T>
bool ExternalSort<T>::mergeChunks(const std::vector<std::string>& chunkNames,
                                  const std::string& output,
                                  unsigned startIndex)
{
    std::cout << "\nMerge chunks " << startIndex << "-"
        << startIndex + chunkNames.size()
        << " to " << output;

    std::ofstream out;
    out.open(output.c_str(), ios::out);

    bool success = mergeChunks(chunkNames, out, startIndex);

    out.close();
    return success;
}

template <class T>
bool ExternalSort<T>::mergeChunks(const std::vector<std::string>& chunkNames,
                                  std::ostream& output,
                                  unsigned startIndex)
{
    // No chunks to merge
    if (chunkNames.empty())
        return false;

    // Open all chunk files
    ChunkFileList chunkFiles = openChunks(chunkNames);

    // Priority queue for the buffer
    std::priority_queue< MergeItem > mergeQueue;

    // Extract the first item from each chunk file
    T item;
    for (size_t i = 0; i < chunkFiles.size(); ++i)
    {
        *chunkFiles[i] >> item;
        mergeQueue.push( MergeItem(item, chunkFiles[i], _cmpFunc) );
    }

    // Process until the queue is empty
    while (!mergeQueue.empty())
    {
        // Take out the top item from the queue to write to output
        MergeItem topItem = mergeQueue.top();
        output << topItem.data << std::endl;
        mergeQueue.pop();

        // Add the next item from the lowest stream (above) to the queue
        *(topItem.stream) >> item;
        if (*(topItem.stream))
            mergeQueue.push( MergeItem(item, topItem.stream, _cmpFunc) );
    }

    // Clean up the chunk files
    cleanupChunks(chunkNames, chunkFiles);

    return true;
}

template <class T>
std::string ExternalSort<T>::writeChunk(const DataBuffer &buffer,
                                        unsigned chunkIndex)
{
    std::ostringstream oss;
    oss << _tempDirPath << "/" << _input
        << ".chunk_" << chunkIndex;
    std::string chunkName = oss.str();

    std::cout << "\nWrite chunk data to " << chunkName;

    std::ofstream output;
    output.open(chunkName.c_str(), ios::out);

    // Write chunk data from buffer to file
    for (size_t i = 0; i < buffer.size(); ++i)
    {
        output << buffer[i] << std::endl;
    }
    output.close();

    // Remember the chunk name
    return chunkName;
}

template <class T>
typename ExternalSort<T>::ChunkFileList ExternalSort<T>::openChunks(
    const std::vector<std::string>& chunkNames)
{
    ChunkFileList chunkFiles;

    for (size_t i = 0; i < chunkNames.size(); ++i)
    {
        const std::string& chunkName = chunkNames[i];
        ifstream_ptr filePtr(new std::ifstream(chunkName.c_str(), ios::in));

        if (filePtr->good())
        {
            chunkFiles.push_back(filePtr);
        }
        else
        {
            cerr << "Error: Unable to open chunk file "
                 << chunkName << std::endl;
            break;
        }
    }

    return chunkFiles;
}

template <class T>
void ExternalSort<T>::cleanupChunks(const std::vector<std::string>& chunkNames,
                                    ChunkFileList& chunkFiles)
{
    // Close the chunk files
    for (size_t i = 0; i < chunkFiles.size(); ++i)
    {
        chunkFiles[i]->close();
    }

    // Delete the chunk files from the file system
    for (size_t i = 0; i < chunkNames.size(); ++i)
    {
        std::remove(chunkNames[i].c_str());
    }
}

#endif /* EXTERNAL_SORT_H_ */
