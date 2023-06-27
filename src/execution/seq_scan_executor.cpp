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
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  auto txn = exec_ctx_->GetTransaction();
  auto iso_level = txn->GetIsolationLevel();
  try {
    if (exec_ctx_->IsDelete()) {
      auto res =
          exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
      if (!res) {
        throw ExecutionException("SeqScanExecutor LockTable Failed");
      }
    } else if (!txn->IsTableIntentionExclusiveLocked(plan_->table_oid_) &&  // 避免反向升级
               (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ)) {
      auto res =
          exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
      if (!res) {
        throw ExecutionException("SeqScanExecutor LockTable Failed");
      }
    }
  } catch (TransactionAbortException &exception) {
    throw ExecutionException("SeqScanExecutor LockTable Failed");
  }
  iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> pair;
  auto txn = exec_ctx_->GetTransaction();
  auto iso_level = txn->GetIsolationLevel();
  while (!iterator_->IsEnd()) {
    pair = iterator_->GetTuple();
    try {
      if (exec_ctx_->IsDelete()) {
        auto res = exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_,
                                                        pair.second.GetRid());
        if (!res) {
          throw ExecutionException("SeqScanExecutor LockRow Failed");
        }
      } else if (!txn->IsRowExclusiveLocked(plan_->table_oid_, pair.second.GetRid()) &&  // 避免反向升级
                 (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ)) {
        auto res = exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_,
                                                        pair.second.GetRid());
        if (!res) {
          throw ExecutionException("SeqScanExecutor LockRow Failed");
        }
      }
    } catch (TransactionAbortException &exception) {
      throw ExecutionException("SeqScanExecutor LockRow Failed");
    }

    if (pair.first.is_deleted_ || (plan_->filter_predicate_ &&
                                   plan_->filter_predicate_->Evaluate(&pair.second, table_info_->schema_)
                                           .CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) {
      if (!exec_ctx_->IsDelete() &&
          (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ)) {
        try {
          auto res = exec_ctx_->GetLockManager()->UnlockRow(txn, plan_->table_oid_, pair.second.GetRid(), true);
          if (!res) {
            throw ExecutionException("SeqScanExecutor Force UnLockRow Failed");
          }
        } catch (TransactionAbortException &exception) {
          throw ExecutionException("SeqScanExecutor Force UnLockRow Failed");
        }
      }
      ++(*iterator_);
      continue;
    }

    if (!exec_ctx_->IsDelete() && iso_level == IsolationLevel::READ_COMMITTED) {
      try {
        auto res = exec_ctx_->GetLockManager()->UnlockRow(txn, plan_->table_oid_, pair.second.GetRid());
        if (!res) {
          throw ExecutionException("SeqScanExecutor UnLockRow Failed");
        }
      } catch (TransactionAbortException &exception) {
        throw ExecutionException("SeqScanExecutor UnLockRow Failed");
      }
    }
    ++(*iterator_);
    *tuple = std::move(pair.second);
    *rid = tuple->GetRid();
    return true;
  }
  if (!exec_ctx_->IsDelete() && iso_level == IsolationLevel::READ_COMMITTED) {
    try {
      auto res = exec_ctx_->GetLockManager()->UnlockTable(txn, plan_->table_oid_);
      if (!res) {
        throw ExecutionException("SeqScanExecutor UnLockTable Failed");
      }
    } catch (TransactionAbortException &exception) {
      throw ExecutionException("SeqScanExecutor UnLockTable Failed");
    }
  }
  return false;
}

}  // namespace bustub
