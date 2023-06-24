//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <type_traits>
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> pair;
  while (!iterator_->IsEnd()) {
    pair = iterator_->GetTuple();
    if (pair.first.is_deleted_) {
      ++(*iterator_);
      continue;
    }
    if (plan_->filter_predicate_) {
      auto res = plan_->filter_predicate_->Evaluate(&pair.second, table_info_->schema_);
      if (!(!res.IsNull() && res.GetAs<bool>())) {
        ++(*iterator_);
        continue;
      }
    }
    ++(*iterator_);
    *tuple = std::move(pair.second);
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
