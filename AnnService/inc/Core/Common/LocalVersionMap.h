// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_LOCALVERSIONMAP_H_
#define _SPTAG_COMMON_LOCALVERSIONMAP_H_

#include "IVersionMap.h"
#include "VersionLabel.h"

namespace SPTAG
{
    namespace COMMON
    {
        /// LocalVersionMap wraps the existing VersionLabel as an IVersionMap.
        /// Used for non-TiKV storage modes (FileIO, RocksDB, SPDK).
        class LocalVersionMap : public IVersionMap
        {
        private:
            VersionLabel m_label;
        public:
            LocalVersionMap() = default;

            void Initialize(SizeType size, SizeType blockSize, SizeType capacity, COMMON::Dataset<SizeType>* globalIDs = nullptr) override
            {
                m_label.Initialize(size, blockSize, capacity, globalIDs);
            }

            void DeleteAll() override { m_label.DeleteAll(); }

            SizeType Count() const override { return m_label.Count(); }
            SizeType GetDeleteCount() const override { return m_label.GetDeleteCount(); }
            SizeType GetVectorNum() override { return m_label.GetVectorNum(); }
            std::uint64_t BufferSize() const override { return m_label.BufferSize(); }

            bool Deleted(const SizeType& key) const override { return m_label.Deleted(key); }
            bool Delete(const SizeType& key) override { return m_label.Delete(key); }

            uint8_t GetVersion(const SizeType& key) override { return m_label.GetVersion(key); }
            void SetVersion(const SizeType& key, const uint8_t& version) override { m_label.SetVersion(key, version); }
            bool IncVersion(const SizeType& key, uint8_t* newVersion, uint8_t expectedOld = 0xff) override { return m_label.IncVersion(key, newVersion); }

            ErrorCode AddBatch(SizeType num) override { return m_label.AddBatch(num); }
            void SetR(SizeType num) override { m_label.SetR(num); }

            ErrorCode Save(std::shared_ptr<Helper::DiskIO> output) override { return m_label.Save(output); }
            ErrorCode Save(const std::string& filename) override { return m_label.Save(filename); }
            ErrorCode Load(std::shared_ptr<Helper::DiskIO> input, SizeType blockSize, SizeType capacity) override { return m_label.Load(input, blockSize, capacity); }
            ErrorCode Load(const std::string& filename, SizeType blockSize, SizeType capacity) override { return m_label.Load(filename, blockSize, capacity); }
            ErrorCode Load(char* pmemoryFile, SizeType blockSize, SizeType capacity) override { return m_label.Load(pmemoryFile, blockSize, capacity); }
        };
    }
}

#endif // _SPTAG_COMMON_LOCALVERSIONMAP_H_
