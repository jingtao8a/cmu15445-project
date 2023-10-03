//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  successful_ = false;
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta tuple_meta;
  if (successful_) {
    return false;
  }
  tuple_meta.delete_txn_id_ = INVALID_TXN_ID;
  tuple_meta.insert_txn_id_ = INVALID_TXN_ID;
  tuple_meta.is_deleted_ = true;
  auto count = 0;
  while (child_executor_->Next(tuple, rid)) {
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);

    auto twr = TableWriteRecord(table_info_->oid_, *rid, table_info_->table_.get());
    twr.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);

    for (auto index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());

      auto iwr = IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, key, index_info->index_oid_,
                                  exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr);
    }
    count++;
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  successful_ = true;
  return true;
}

}  // namespace bustub
