/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/syscall.h>
#include "velox/tpch/gen/TpchGen.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;
using namespace facebook::velox::tpch;
using std::chrono::system_clock;

DEFINE_int32(SCALE_FACTOR, 10, "scale factor");
DEFINE_int32(MAX_ORDER_ROWS, 6000000, "max order rows");
DEFINE_int32(BASE_SCALE_FACTOR, 1, "scale factor used in tpchgen");

DEFINE_string(Compression, "zstd", "parquet compression");
DEFINE_int32(Level, 1, "compression level");
DEFINE_string(SaveDirectory, "/tmp", "parquet save directory");

class ParquetWriterBenchmark {
 public:
  ParquetWriterBenchmark(bool disableDictionary)
      : disableDictionary_(disableDictionary) {
    rootPool_ = memory::defaultMemoryManager().addRootPool("ParquetReaderBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetReaderBenchmark");

    int id = syscall(SYS_gettid);
    auto path = FLAGS_SaveDirectory + "/" + FLAGS_Compression + "_compressed_" + std::to_string(FLAGS_Level) + "_level_"
        + std::to_string(id) + ".parquet";
    auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(localWriteFile), path);

    facebook::velox::parquet::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.compression = getCompressionType(FLAGS_Compression);
    if(CompressionKind_NONE != options.compression)
    {
      options.codecOptions = std::make_shared<CodecOptions>();
      options.codecOptions->compression_level = FLAGS_Level;
    }
	  options.schema = getTableSchema(Table::TBL_LINEITEM);
    if (disableDictionary_) {
      // The parquet file is in plain encoding format.
      options.enableDictionary = false;
    }
    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), options);
  }

  ~ParquetWriterBenchmark() {
    writer_->close();
  }

  void writeToFile(void) {
    RowVectorPtr rowVector;
	int total_writes = FLAGS_SCALE_FACTOR / FLAGS_BASE_SCALE_FACTOR;

    folly::BenchmarkSuspender suspender;

    rowVector = facebook::velox::tpch::genTpchLineItem(
        leafPool_.get(), FLAGS_MAX_ORDER_ROWS, 0, FLAGS_BASE_SCALE_FACTOR);

    suspender.dismiss();

    for (int i = 0; i < total_writes; i++) {
      LOG(INFO) << "i: " << i << ", num row: " << rowVector->size()
                << std::endl;
      writer_->write(rowVector);
    }

    suspender.rehire();

    LOG(INFO) << "success write." << std::endl;

    writer_->flush();
  }

  CompressionKind getCompressionType(std::string compression) {
    std::string comType = compression;

    LOG(INFO) << "compression: " << compression << std::endl;

    std::transform(comType.begin(), comType.end(), comType.begin(), ::tolower);

    if (comType.compare("snappy") == 0) {
      return CompressionKind_SNAPPY;
    } else if (comType.compare("zstd") == 0) {
      return CompressionKind_ZSTD;
    } else if (comType.compare("lz4") == 0) {
      return CompressionKind_LZ4;
    } else if (comType.compare("gzip") == 0) {
      return CompressionKind_GZIP;
    } else {
      return CompressionKind_NONE;
    }
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  const bool disableDictionary_;
};

void runParquetWrite(void) {
  ParquetWriterBenchmark benchmark(true);
  benchmark.writeToFile();
}

BENCHMARK(parquetWriteBenchmark) {
   runParquetWrite();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
