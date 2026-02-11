#include "inc/TestDataGenerator.h"
#include <cstring>
#include <iostream>
#include <sstream>

using namespace SPTAG;

namespace TestUtils
{

template <typename T>
TestDataGenerator<T>::TestDataGenerator(int n, int q, int m, int k, std::string distMethod, int a, bool isRandom,
                                        std::string vectorPath, std::string queryPath)
    : m_n(n), m_a((a == 0)? n : a), m_q(q), m_m(m), m_k(k), m_distMethod(std::move(distMethod)), m_isRandom(isRandom),
      m_vectorPath(vectorPath), m_queryPath(queryPath)
{
}

template <typename T>
void TestDataGenerator<T>::RunBatches(std::shared_ptr<SPTAG::VectorSet>& vecset,
    std::shared_ptr<SPTAG::MetadataSet>& metaset,
    std::shared_ptr<SPTAG::VectorSet>& addvecset, std::shared_ptr<SPTAG::MetadataSet>& addmetaset,
    std::shared_ptr<SPTAG::VectorSet>& queryset, int base, int batchinsert, int batchdelete, int batches,
    std::shared_ptr<SPTAG::VectorSet>& truths)
{
    std::string pvecset, pmetaset, pmetaidx, paddset, paddmetaset, paddmetaidx, pqueryset, ptruth;
    RunLargeBatches(pvecset, pmetaset, pmetaidx, paddset, paddmetaset, paddmetaidx, pqueryset, base, batchinsert, batchdelete, batches, ptruth, true); 
    vecset = LoadVectorSet(pvecset, m_m, 0, m_n);
    metaset = LoadMetadataSet(pmetaset, pmetaidx, 0, m_n);
    addvecset = LoadVectorSet(paddset, m_m, 0, m_a);
    addmetaset = LoadMetadataSet(paddmetaset, paddmetaidx, 0, m_a);
    queryset = LoadVectorSet(pqueryset, m_m, 0, m_q); 
    truths = TestDataGenerator<float>::LoadVectorSet(ptruth, m_k);
}

template <typename T>
void TestDataGenerator<T>::RunLargeBatches(std::string &vecset, std::string &metaset, std::string &metaidx,
                                           std::string &addset, std::string &addmetaset, std::string &addmetaidx,
                                           std::string &queryset, int base, int batchinsert, int batchdelete,
                                           int batches, std::string &truth, bool generateTruth)
{
    vecset = "perftest_vector.bin." + SPTAG::Helper::Convert::ConvertToString(GetEnumValueType<T>()) + "_" + std::to_string(m_n) + "_" + std::to_string(m_m);
    metaset = "perftest_meta.bin." + std::to_string(0) + "_" + std::to_string(m_n);
    metaidx = "perftest_metaidx.bin." + std::to_string(0) + "_" + std::to_string(m_n);
    addset = "perftest_addvector.bin." + SPTAG::Helper::Convert::ConvertToString(GetEnumValueType<T>()) + "_" + std::to_string(m_a) + "_" + std::to_string(m_m);
    addmetaset = "perftest_addmeta.bin." + std::to_string(m_n) + "_" + std::to_string(m_a);
    addmetaidx = "perftest_addmetaidx.bin." + std::to_string(m_n) + "_" + std::to_string(m_a);
    queryset = "perftest_query.bin." + SPTAG::Helper::Convert::ConvertToString(GetEnumValueType<T>()) + "_" + std::to_string(m_q) + "_" + std::to_string(m_m);
    truth = "perftest_batchtruth." + m_distMethod + "." + SPTAG::Helper::Convert::ConvertToString(GetEnumValueType<T>()) + "_" + std::to_string(base) + "_" + std::to_string(m_m) + 
            "_" + std::to_string(m_q) + "_" + std::to_string(m_k) + "_" + std::to_string(batchinsert) + "_" + std::to_string(batchdelete) + "_" + std::to_string(batches);
    std::string empty;

    GenerateVectorSet(vecset, metaset, metaidx, m_vectorPath, 0, m_n);
    GenerateVectorSet(queryset, empty, empty, m_queryPath, 0, m_q);
    GenerateVectorSet(addset, addmetaset, addmetaidx, m_vectorPath, m_n, m_a);
    if (generateTruth)
    {
        GenerateBatchTruth(truth, vecset, addset, queryset, base, batchinsert, batchdelete, batches, true);
    }
}

template<typename T>
std::shared_ptr<SPTAG::VectorSet> TestDataGenerator<T>::LoadVectorSet(const std::string pvecset, DimensionType dim, SPTAG::SizeType start, SPTAG::SizeType count)
{
    auto vectorOptions = std::shared_ptr<Helper::ReaderOptions>(new Helper::ReaderOptions(GetEnumValueType<T>(), dim, VectorFileType::DEFAULT));
    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
    if (!fileexists(pvecset.c_str()) || ErrorCode::Success != vectorReader->LoadFile(pvecset))
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot find or load %s!\n", pvecset.c_str());
        return nullptr;
    }
    return vectorReader->GetVectorSet(start, start + count);
}

template<typename T>
std::shared_ptr<SPTAG::MetadataSet> TestDataGenerator<T>::LoadMetadataSet(const std::string pmetaset, const std::string pmetaidx, SPTAG::SizeType start, SPTAG::SizeType count)
{
    std::shared_ptr<SPTAG::MetadataSet> metaset(new MemMetadataSet(pmetaset, pmetaidx, 1024 * 1024, MaxSize, 10, start, count));
    return metaset;
}

template<typename T>
void TestDataGenerator<T>::GenerateVectorSet(std::string & pvecset, std::string & pmetaset, std::string & pmetaidx, std::string& pvecPath, SPTAG::SizeType start, int count)
{
    if (!fileexists(pvecset.c_str()))
    {
        std::shared_ptr<SPTAG::VectorSet> vecset;
        if (m_isRandom)
        {
            vecset = GenerateRandomVectorSet(count, m_m);
        }
        else
        {
            vecset = GenerateLoadVectorSet(count, m_m, pvecPath, start);
        }
        vecset->Save(pvecset);
    }

    if (pmetaset.empty() || pmetaidx.empty())
        return;

    if (!fileexists(pmetaset.c_str()) || !fileexists(pmetaidx.c_str()))
    {
        auto metaset = GenerateMetadataSet(count, start);
        metaset->SaveMetadata(pmetaset, pmetaidx);
    }
}

template <typename T>
void TestDataGenerator<T>::GenerateBatchTruth(const std::string &filename, std::string &pvecset, std::string &paddset, std::string &pqueryset, int base,
                                                    int batchinsert, int batchdelete, int batches, bool normalize)
{
    if (fileexists(filename.c_str()))
        return;

    auto vecset = LoadVectorSet(pvecset, m_m);
    auto queryset = LoadVectorSet(pqueryset, m_m);
    auto addset = LoadVectorSet(paddset, m_m);


    DistCalcMethod distMethod;
    Helper::Convert::ConvertStringTo(m_distMethod.c_str(), distMethod);
    if (normalize && distMethod == DistCalcMethod::Cosine)
    {
        COMMON::Utils::BatchNormalize((T *)vecset->GetData(), vecset->Count(), vecset->Dimension(),
                                      COMMON::Utils::GetBase<T>(), 5);
        COMMON::Utils::BatchNormalize((T *)addset->GetData(), addset->Count(), addset->Dimension(),
                                      COMMON::Utils::GetBase<T>(), 5);
    }

    ByteArray tru = ByteArray::Alloc((sizeof(float) + sizeof(SizeType)) * (batches + 1) * queryset->Count() * m_k);
    int distbase = sizeof(SizeType) * (batches + 1) * queryset->Count() * m_k;
    int start = 0;
    int end = base;
    int maxthreads = std::thread::hardware_concurrency();
    for (int iter = 0; iter < batches + 1; iter++)
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Generating groundtruth for batch %d\n", iter);
        std::vector<std::thread> mythreads;
        mythreads.reserve(maxthreads);
        std::atomic_size_t sent(0);
        for (int tid = 0; tid < maxthreads; tid++)
        {
            mythreads.emplace_back([&, tid]() {
                size_t i = 0;
                while (true)
                {
                    i = sent.fetch_add(1);
                    if (i < queryset->Count())
                    {
                        SizeType *neighbors = ((SizeType *)tru.Data()) + iter * (queryset->Count() * m_k) + i * m_k;
                        float *dists = ((float *)(tru.Data() + distbase)) + iter * (queryset->Count() * m_k) + i * m_k;
                        COMMON::QueryResultSet<T> res((const T *)queryset->GetVector(i), m_k);
                        for (SizeType j = start; j < end; ++j)
                        {
                            float dist = MaxDist;
                            if (j < vecset->Count())
                                dist = COMMON::DistanceUtils::ComputeDistance(res.GetTarget(), 
                                    reinterpret_cast<T *>(vecset->GetVector(j)), m_m, distMethod);
                            else
                                dist = COMMON::DistanceUtils::ComputeDistance(res.GetTarget(), 
                                    reinterpret_cast<T *>(addset->GetVector(j - vecset->Count())), m_m, distMethod);

                            res.AddPoint(j, dist);
                        }
                        res.SortResult();
                        for (int j = 0; j < m_k; ++j)
                        {
                            neighbors[j] = res.GetResult(j)->VID;
                            dists[j] = res.GetResult(j)->Dist;
                        }
                    }
                    else
                    {
                        return;
                    }
                }
            });
        }
        for (auto &t : mythreads)
        {
            t.join();
        }
        mythreads.clear();
        start += batchdelete;
        end += batchinsert;
    }
    auto truths = std::make_shared<BasicVectorSet>(tru, GetEnumValueType<float>(), m_k, ((sizeof(float) + sizeof(SizeType)) / sizeof(float)) * (batches + 1) * queryset->Count());
    truths->Save(filename);
}

template <typename T>
float TestDataGenerator<T>::EvaluateRecall(const std::vector<SPTAG::QueryResult> &res, std::shared_ptr<SPTAG::VectorSet> &truth, int recallK, int k, int batch, int totalbatches)
{
    if (!truth)
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Truth data is null. Cannot compute recall.\n");
        return 0.0f;
    }

    recallK = min(recallK, static_cast<int>(truth->Dimension()));
    float totalRecall = 0.0f;
    float eps = 1e-4f;
    SizeType distbase = truth->Count() - (totalbatches + 1) * res.size();
    for (SizeType i = 0; i < res.size(); ++i)
    {
        const SizeType *truthNN = reinterpret_cast<const SizeType *>(truth->GetData()) + batch * res.size() + i;
        float *truthD = nullptr;
        if (truth->Count() > distbase)
        {
            truthD = reinterpret_cast<float *>(truth->GetVector(distbase + batch * res.size() + i));
        }
        for (int j = 0; j < recallK; ++j)
        {
            SizeType truthVid = truthNN[j];
            float truthDist = MaxDist;
            if (truthD)
            {
                truthDist = truthD[j];
            }
            
            for (int l = 0; l < k; ++l)
            {
                const auto result = res[i].GetResult(l);
                if (truthVid == result->VID ||
                    std::fabs(truthDist - result->Dist) <= eps * (std::fabs(truthDist) + eps))
                {
                    totalRecall += 1.0f;
                    break;
                }
            }
        }
    }

    float avgRecall = totalRecall / (res.size() * recallK);
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Recall %d@%d = %.4f\n", recallK, k, avgRecall);
    return avgRecall;
}

template <typename T>
std::shared_ptr<VectorSet> TestDataGenerator<T>::GenerateRandomVectorSet(SizeType count, DimensionType dim)
{
    ByteArray vec = ByteArray::Alloc(sizeof(T) * count * dim);
    for (SizeType i = 0; i < count * dim; ++i)
    {
        ((T *)vec.Data())[i] = (T)COMMON::Utils::rand(127, -127);
    }
    return std::make_shared<BasicVectorSet>(vec, GetEnumValueType<T>(), dim, count);
}

template <typename T>
std::shared_ptr<MetadataSet> TestDataGenerator<T>::GenerateMetadataSet(SizeType count, SizeType offsetBase)
{
    ByteArray meta = ByteArray::Alloc(count * 10);
    ByteArray metaoffset = ByteArray::Alloc((count + 1) * sizeof(std::uint64_t));
    std::uint64_t offset = 0;
    for (SizeType i = 0; i < count; i++)
    {
        ((std::uint64_t *)metaoffset.Data())[i] = offset;
        std::string id = std::to_string(i + offsetBase);
        std::memcpy(meta.Data() + offset, id.c_str(), id.length());
        offset += id.length();
    }
    ((std::uint64_t *)metaoffset.Data())[count] = offset;
    return std::make_shared<MemMetadataSet>(meta, metaoffset, count, 1024 * 1024, MaxSize, 10);
}

template <typename T>
std::shared_ptr<SPTAG::VectorSet> TestDataGenerator<T>::GenerateLoadVectorSet(SPTAG::SizeType count,
                                                                              SPTAG::DimensionType dim,
                                                                              std::string path, SPTAG::SizeType start)
{
    VectorFileType fileType = VectorFileType::DEFAULT;
    if (path.find(".fvecs") != std::string::npos || path.find(".ivecs") != std::string::npos)
    {
        fileType = VectorFileType::XVEC;
    }
    auto vectorOptions =
        std::shared_ptr<Helper::ReaderOptions>(new Helper::ReaderOptions(GetEnumValueType<T>(), dim, fileType));
    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);

    if (!fileexists(path.c_str()) || ErrorCode::Success != vectorReader->LoadFile(path))
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot find or load %s. Using random generation!\n", path.c_str());
        return GenerateRandomVectorSet(count, dim);
    }

    auto allVectors = vectorReader->GetVectorSet(start, start + count);
    if (allVectors->Count() < count)
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                        "Cannot get %d vectors start from %d. Using random generation!\n", count, start);
        return GenerateRandomVectorSet(count, dim);
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load %d vectors start from %d\n", count, start);
    return allVectors;
}


// Explicit instantiation
template class TestDataGenerator<int8_t>;
template class TestDataGenerator<uint8_t>;
template class TestDataGenerator<int16_t>;
template class TestDataGenerator<float>;
} // namespace TestUtils
