#ifndef _SPTAG_HELPER_KEYVALUEIO_H_
#define _SPTAG_HELPER_KEYVALUEIO_H_

#include "inc/Core/Common.h"
#include "inc/Helper/DiskIO.h"
#include <vector>
#include <chrono>

namespace SPTAG
{
    namespace Helper
    {
        /// Describes which store/shard holds a given key (for distributed routing).
        struct KeyLocation {
            uint64_t regionId = 0;
            std::string leaderStoreAddr;
        };

        class KeyValueIO {
        public:
            KeyValueIO() {}

            virtual ~KeyValueIO() {}

            virtual void ShutDown() = 0;

            virtual ErrorCode Get(const std::string& key, std::string* value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) { return ErrorCode::Undefined; }

            virtual ErrorCode Get(const SizeType key, std::string* value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) = 0;

            virtual ErrorCode Get(const SizeType key, Helper::PageBuffer<std::uint8_t> &value, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest> *reqs, bool useCache = true) { return ErrorCode::Undefined; }

            virtual ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<SPTAG::Helper::PageBuffer<std::uint8_t>>& values, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) { return ErrorCode::Undefined; }

            virtual ErrorCode MultiGet(const std::vector<SizeType>& keys, std::vector<std::string>* values, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) = 0;

            virtual ErrorCode MultiGet(const std::vector<std::string>& keys, std::vector<std::string>* values, const std::chrono::microseconds &timeout, std::vector<Helper::AsyncReadRequest>* reqs) { return ErrorCode::Undefined; }            
 
            virtual ErrorCode Put(const std::string& key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) { return ErrorCode::Undefined; }

            virtual ErrorCode Put(const SizeType key, const std::string& value, const std::chrono::microseconds& timeout, std::vector<Helper::AsyncReadRequest>* reqs) = 0;

            virtual ErrorCode Merge(const SizeType key, const std::string &value,
                                    const std::chrono::microseconds &timeout,
                                    std::vector<Helper::AsyncReadRequest> *reqs, int& size) = 0;

            /// Batch put multiple key-value pairs in one round-trip.
            virtual ErrorCode BatchPut(const std::vector<SizeType>& keys,
                                       const std::vector<std::string>& values,
                                       const std::chrono::microseconds& timeout) {
                // Default: fall back to individual Puts
                for (size_t i = 0; i < keys.size(); i++) {
                    auto ret = Put(keys[i], values[i], timeout, nullptr);
                    if (ret != ErrorCode::Success) return ret;
                }
                return ErrorCode::Success;
            }

            /// Batch merge (read-modify-write) multiple keys in one round-trip.
            /// Reads all keys via MultiGet, appends values in memory, writes
            /// back via BatchPut.  Reduces 2N serial RPCs to 2 batch RPCs.
            /// @param[out] sizes  Resulting posting sizes after merge.
            virtual ErrorCode BatchMerge(const std::vector<SizeType>& keys,
                                         const std::vector<std::string>& values,
                                         const std::chrono::microseconds& timeout,
                                         std::vector<int>& sizes) {
                // Default: fall back to individual Merges
                sizes.resize(keys.size());
                for (size_t i = 0; i < keys.size(); i++) {
                    auto ret = Merge(keys[i], values[i], timeout, nullptr, sizes[i]);
                    if (ret != ErrorCode::Success) return ret;
                }
                return ErrorCode::Success;
            }

            virtual ErrorCode Delete(SizeType key) = 0;

            virtual ErrorCode DeleteRange(SizeType start, SizeType end) {return ErrorCode::Undefined;}

            virtual void ForceCompaction() {}

            virtual void GetStat() {}

            virtual int64_t GetNumBlocks() { return 0; }
            
            virtual bool Available() { return false; }

            virtual ErrorCode Check(const SizeType key, std::vector<std::uint8_t> *visited)
            {
                return ErrorCode::Undefined;
            }

            virtual int64_t GetApproximateMemoryUsage()
            {
                return 0;
            }

            virtual ErrorCode Checkpoint(std::string prefix) {return ErrorCode::Undefined;}

            virtual ErrorCode StartToScan(SizeType& key, std::string* value) {return ErrorCode::Undefined;}

            virtual ErrorCode NextToScan(SizeType& key, std::string* value) {return ErrorCode::Undefined;}

            // Distributed routing support (TODO2): query which store holds a key.
            virtual bool GetKeyLocation(SizeType key, KeyLocation& loc) { return false; }
            virtual bool GetKeyLocations(const std::vector<SizeType>& keys,
                                         std::vector<KeyLocation>& locs) { return false; }
        };
    }
}

#endif