//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  table_info_ = catalog->GetTable(index_info_->table_name_);
  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  index_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!index_iterator_->IsEnd()) {
    auto map = *(*index_iterator_);
    *rid = map.second;
    if (!table_info_->table_->GetTupleMeta(*rid).is_deleted_) {  // 未被删除
      index_iterator_->operator++();
      *tuple = table_info_->table_->GetTuple(*rid).second;
      return true;
    }
    index_iterator_->operator++();
  }
  return false;
}

}  // namespace bustub
