//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "execution/executors/abstract_executor.h"
#include "execution/executors/update_executor.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  successful_ = false;
  auto cata_log = exec_ctx_->GetCatalog();
  table_info_ = cata_log->GetTable(plan_->TableOid());
  index_infos_ = cata_log->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta tuple_meta;
  if (successful_) {
    return false;
  }
  tuple_meta.delete_txn_id_ = INVALID_TXN_ID;
  tuple_meta.insert_txn_id_ = INVALID_TXN_ID;
  auto count = 0;
  while (child_executor_->Next(tuple, rid)) {
    // 删除tuple
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    // 计算新的tuple
    std::vector<Value> values;
    for (auto &expresssion : plan_->target_expressions_) {
      values.emplace_back(expresssion->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    // 插入新的tuple
    tuple_meta.is_deleted_ = false;
    auto tuple_rid = table_info_->table_->InsertTuple(tuple_meta, new_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), table_info_->oid_);
    if (tuple_rid == std::nullopt) {
      continue;
    }
    for (auto index_info : index_infos_) {
      auto key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *tuple_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = {values, &GetOutputSchema()};
  successful_ = true;
  return true;
}

}  // namespace bustub
