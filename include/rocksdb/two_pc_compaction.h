#pragma once

#include <stdint.h>
#include <climits>
#include <vector>

namespace rocksdb {

class CompactionOptionsTwoPC {
 public:

  int start_level;
  size_t merge_threshold;

  // Default set of parameters
  CompactionOptionsTwoPC()
      : start_level(1),
        merge_threshold(5) {}
};
//uint64_t TwoPCStatic::compaction_input_size = 0;
//uint64_t TwoPCStatic::compaction_output_size = 0;

}  // namespace rocksdb
