// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/ResultIterator.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Test.h"
#include "inc/TestDataGenerator.h"

#include <chrono>
#include <unordered_set>

using namespace SPTAG;

template <typename T>
void BuildIndex(IndexAlgoType algo, std::string distCalcMethod, std::shared_ptr<VectorSet> &vec,
                std::shared_ptr<MetadataSet> &meta, const std::string out)
{
    std::shared_ptr<VectorIndex> vecIndex =
        VectorIndex::CreateInstance(algo, GetEnumValueType<T>());
    BOOST_CHECK(nullptr != vecIndex);

    if (algo != IndexAlgoType::SPANN)
    {
        vecIndex->SetParameter("DistCalcMethod", distCalcMethod);
        vecIndex->SetParameter("NumberOfThreads", "16");
    }
    else
    {
        vecIndex->SetParameter("IndexAlgoType", "BKT", "Base");
        vecIndex->SetParameter("DistCalcMethod", distCalcMethod, "Base");

        vecIndex->SetParameter("isExecute", "true", "SelectHead");
        vecIndex->SetParameter("NumberOfThreads", "4", "SelectHead");
        vecIndex->SetParameter("Ratio", "0.2", "SelectHead"); // vecIndex->SetParameter("Count", "200", "SelectHead");

        vecIndex->SetParameter("isExecute", "true", "BuildHead");
        vecIndex->SetParameter("RefineIterations", "3", "BuildHead");
        vecIndex->SetParameter("NumberOfThreads", "4", "BuildHead");

        vecIndex->SetParameter("isExecute", "true", "BuildSSDIndex");
        vecIndex->SetParameter("BuildSsdIndex", "true", "BuildSSDIndex");
        vecIndex->SetParameter("NumberOfThreads", "4", "BuildSSDIndex");
        vecIndex->SetParameter("PostingPageLimit", std::to_string(4 * sizeof(T)), "BuildSSDIndex");
        vecIndex->SetParameter("SearchPostingPageLimit", std::to_string(4 * sizeof(T)), "BuildSSDIndex");
        vecIndex->SetParameter("InternalResultNum", "64", "BuildSSDIndex");
        vecIndex->SetParameter("SearchInternalResultNum", "64", "BuildSSDIndex");
        vecIndex->SetParameter("MaxCheck", "8192", "BuildSSDIndex");
    }

    BOOST_CHECK(ErrorCode::Success == vecIndex->BuildIndex(vec, meta));
    BOOST_CHECK(ErrorCode::Success == vecIndex->SaveIndex(out));
}

template <typename T>
void SearchIterativeBatch(const std::string folder, T *vec, SizeType n, std::string *truthmeta)
{
    std::shared_ptr<VectorIndex> vecIndex;

    BOOST_CHECK(ErrorCode::Success == VectorIndex::LoadIndex(folder, vecIndex));
    BOOST_CHECK(nullptr != vecIndex);
    vecIndex->SetParameter("MaxCheck", "5", "BuildSSDIndex");
    vecIndex->UpdateIndex();

    std::shared_ptr<ResultIterator> resultIterator = vecIndex->GetIterator(vec);
    // std::cout << "relaxedMono:" << resultIterator->GetRelaxedMono() << std::endl;
    int batch = 5;
    int ri = 0;
    for (int i = 0; i < 2; i++)
    {
        auto results = resultIterator->Next(batch);
        int resultCount = results->GetResultNum();
        if (resultCount <= 0)
            break;
        for (int j = 0; j < resultCount; j++)
        {

            BOOST_CHECK(std::string((char *)((results->GetMetadata(j)).Data()), (results->GetMetadata(j)).Length()) ==
                        truthmeta[ri]);
            BOOST_CHECK(results->GetResult(j)->RelaxedMono == true);
            std::cout << "Result[" << ri << "] VID:" << results->GetResult(j)->VID
                      << " Dist:" << results->GetResult(j)->Dist
                      << " RelaxedMono:" << results->GetResult(j)->RelaxedMono << std::endl;
            ri++;
        }
    }
    resultIterator->Close();
}

template <typename T> void TestIterativeScan(IndexAlgoType algo, std::string distCalcMethod)
{
    SizeType n = 6000, q = 1;
    DimensionType m = 10;
    std::vector<T> vec;
    for (SizeType i = 0; i < n; i++)
    {
        for (DimensionType j = 0; j < m; j++)
        {
            vec.push_back((T)i);
        }
    }

    std::vector<T> query;
    for (SizeType i = 0; i < q; i++)
    {
        for (DimensionType j = 0; j < m; j++)
        {
            query.push_back((T)i * 2);
        }
    }

    std::vector<char> meta;
    std::vector<std::uint64_t> metaoffset;
    for (SizeType i = 0; i < n; i++)
    {
        metaoffset.push_back((std::uint64_t)meta.size());
        std::string a = std::to_string(i);
        for (size_t j = 0; j < a.length(); j++)
            meta.push_back(a[j]);
    }
    metaoffset.push_back((std::uint64_t)meta.size());

    std::shared_ptr<VectorSet> vecset(new BasicVectorSet(
        ByteArray((std::uint8_t *)vec.data(), sizeof(T) * n * m, false), GetEnumValueType<T>(), m, n));

    std::shared_ptr<MetadataSet> metaset(new MemMetadataSet(
        ByteArray((std::uint8_t *)meta.data(), meta.size() * sizeof(char), false),
        ByteArray((std::uint8_t *)metaoffset.data(), metaoffset.size() * sizeof(std::uint64_t), false), n));

    BuildIndex<T>(algo, distCalcMethod, vecset, metaset, "testindices");
    std::string truthmeta1[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
    SearchIterativeBatch<T>("testindices", query.data(), q, truthmeta1);
}

template <typename T> void TestIterativeScanRandom(IndexAlgoType algo, std::string distCalcMethod)
{
    SizeType n = 50000, q = 3, k = 5;
    DimensionType m = 128;
    std::shared_ptr<VectorSet> vecset, queryset, truth, addvecset;
    std::shared_ptr<MetadataSet> metaset, addmetaset;

    TestUtils::TestDataGenerator<int8_t> generator(n, q, m, k, distCalcMethod);
    generator.RunBatches(vecset, metaset, addvecset, addmetaset, queryset, n, n, 0, 1, truth);

    BuildIndex<T>(algo, distCalcMethod, vecset, metaset, "testindices");
    std::shared_ptr<VectorIndex> vecIndex;
    BOOST_CHECK(ErrorCode::Success == VectorIndex::LoadIndex("testindices", vecIndex));
    BOOST_CHECK(nullptr != vecIndex);

    std::vector<QueryResult> res(queryset->Count(), QueryResult(nullptr, k, true));
    std::vector<QueryResult> resiter(queryset->Count(), QueryResult(nullptr, k, true));
    for (int i = 0; i < q; i++)
    {
        res[i].SetTarget(queryset->GetVector(i));
        vecIndex->SearchIndex(res[i]);
        int scanned = res[i].GetScanned();

        std::shared_ptr<ResultIterator> resultIterator = vecIndex->GetIterator((T *)(queryset->GetVector(i)));
        int batch = 1;
        int ri = 0;
        int iterscanned = 0;
        bool relaxMono = false;
        while (!relaxMono)
        {
            auto results = resultIterator->Next(batch);
            int resultCount = results->GetResultNum();
            if (resultCount <= 0) break;
            for (int j = 0; j < resultCount; j++)
            {
                relaxMono = results->GetResult(j)->RelaxedMono;
                ((COMMON::QueryResultSet<T> *)(&resiter[i]))->AddPoint(results->GetResult(j)->VID, results->GetResult(j)->Dist);
            }
            ri += resultCount;
            iterscanned = results->GetScanned();
        }
        resultIterator->Close();

        std::cout << "TopK scanned:" << scanned << " Iterator scanned:" << iterscanned << std::endl;
    }
    std::cout << "TopK Recall:" << TestUtils::TestDataGenerator<T>::EvaluateRecall(res, truth, k, k, 0, 1)
              << " Iterator Recall:" << TestUtils::TestDataGenerator<T>::EvaluateRecall(resiter, truth, k, k, 0, 1) << std::endl;
}

BOOST_AUTO_TEST_SUITE(IterativeScanTest)

BOOST_AUTO_TEST_CASE(BKTTest)
{
    TestIterativeScan<float>(IndexAlgoType::BKT, "L2");
}

BOOST_AUTO_TEST_CASE(BKTRandomTest)
{
    TestIterativeScanRandom<int8_t>(IndexAlgoType::BKT, "L2");
}

BOOST_AUTO_TEST_CASE(SPANNRandomTest)
{
    TestIterativeScanRandom<int8_t>(IndexAlgoType::SPANN, "L2");
}

BOOST_AUTO_TEST_SUITE_END()
