//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "catalog/column.h"
#include "common/config.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto cata_log = exec_ctx_->GetCatalog();
  table_info_ = cata_log->GetTable(plan_->table_oid_);
  index_infos_ = cata_log->GetTableIndexes(table_info_->name_);
  successful_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta;
  if (successful_) {
    return false;
  }
  meta.insert_txn_id_ = INVALID_TXN_ID;
  meta.delete_txn_id_ = INVALID_TXN_ID;
  meta.is_deleted_ = false;
  auto count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto tuple_rid = table_info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), table_info_->oid_);
    if (tuple_rid == std::nullopt) {
      continue;
    }
    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *tuple_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  successful_ = true;
  return true;
}

}  // namespace bustub
