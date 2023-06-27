//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
  std::unique_lock<std::shared_mutex> guard(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    auto twr = txn->GetWriteSet()->back();
    if (twr.wtype_ == WType::INSERT) {
      auto tuple_meta = twr.table_heap_->GetTupleMeta(twr.rid_);
      tuple_meta.is_deleted_ = true;
      twr.table_heap_->UpdateTupleMeta(tuple_meta, twr.rid_);
    } else if (twr.wtype_ == WType::DELETE) {
      auto tuple_meta = twr.table_heap_->GetTupleMeta(twr.rid_);
      tuple_meta.is_deleted_ = false;
      twr.table_heap_->UpdateTupleMeta(tuple_meta, twr.rid_);
    }
    txn->GetWriteSet()->pop_back();
  }

  while (!txn->GetIndexWriteSet()->empty()) {
    auto iwr = txn->GetIndexWriteSet()->back();
    if (iwr.wtype_ == WType::INSERT) {
      iwr.catalog_->GetIndex(iwr.index_oid_)->index_->DeleteEntry(iwr.tuple_, iwr.rid_, txn);
    } else if (iwr.wtype_ == WType::DELETE) {
      iwr.catalog_->GetIndex(iwr.index_oid_)->index_->InsertEntry(iwr.tuple_, iwr.rid_, txn);
    }
    txn->GetIndexWriteSet()->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
  std::unique_lock<std::shared_mutex> guard(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
