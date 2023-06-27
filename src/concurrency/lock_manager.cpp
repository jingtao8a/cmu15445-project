//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <bits/types/stack_t.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

#define ABORT_FOR_REASON_DIRECTLY_OR_NOT(X, directly)            \
  do {                                                           \
    if (!(directly)) {                                           \
      return false;                                              \
    }                                                            \
    txn->SetState(TransactionState::ABORTED);                    \
    throw TransactionAbortException(txn->GetTransactionId(), X); \
  } while (0)

namespace bustub {
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return LockTableDirectlyOrNot(txn, lock_mode, oid, true);
}

auto LockManager::LockTableDirectlyOrNot(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool directly)
    -> bool {
  auto txn_state = txn->GetState();
  auto iso_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED) {
    return false;
  }
  switch (iso_level) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn_state == TransactionState::SHRINKING) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, directly);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, directly);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, directly);
      }
      if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, directly);
      }
      break;
    default:
      UNREACHABLE("wrong IsolationLevel");
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  txn_wait_map_[txn->GetTransactionId()] = lrq;
  table_lock_map_latch_.unlock();

  // 检查此锁的请求是否为一次锁升级
  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {  // 同一个事务对相同table请求加锁
      if (lr->lock_mode_ == lock_mode) {           // 加锁的类型相同,直接返回
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {  // 有事务正在对该resource进行锁升级
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::UPGRADE_CONFLICT, directly);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {  // 不能够进行锁升级
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::INCOMPATIBLE_UPGRADE, directly);
      }
      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(iter);
      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);
      delete lr;  // 防止内存泄露
      lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
      upgrade = true;
      break;
    }
  }
  // 不是锁升级
  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }

  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
    // 可能死锁检测将该事务ABORTED 或者 手动ABORT该事务
    if (txn->GetState() == TransactionState::ABORTED) {
      // 删除该事务对该资源的request
      for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
        auto lr = *iter;
        if (lr->txn_id_ == txn->GetTransactionId()) {
          lrq->request_queue_.erase(iter);
          delete lr;
          break;
        }
      }
      lrq->cv_.notify_all();
      return false;
    }
  }

  AddIntoTxnTableLockSet(txn, lock_mode, oid);
  // LOG_DEBUG("txn:%d LockTable %d lock_mode:%d", txn->GetTransactionId(), oid, lock_mode);
  return true;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode,
                                 std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto lr : lock_request_queue->request_queue_) {
    if (lr->granted_ && !AreLocksCompatible(lock_mode, lr->lock_mode_)) {  // 存在锁冲突
      return false;
    }
  }
  // 锁升级优先级最高
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {  // 事务进行锁升级
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      for (auto lr : lock_request_queue->request_queue_) {
        if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
          lr->granted_ = true;
          break;
        }
      }
      return true;
    }
    return false;  // 进行锁升级的是其它事务,那么该事务需要等待
  }
  // 遵循FIFO规则
  for (auto lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lr->granted_ = true;
      break;
    }
    if (!lr->granted_ && !AreLocksCompatible(lock_mode, lr->lock_mode_)) {  // 锁冲突
      return false;
    }
  }
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {  // IS->[S, X, IX, SIX]
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED ||
           requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::SHARED) {  // S -> [X, SIX]
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {  // IX -> [X, SIX]
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {  // SIX -> [X]
    return requested_lock_mode == LockMode::EXCLUSIVE;
  }

  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!CheckAllRowsUnLock(txn, oid)) {
    ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, true);
  }

  table_lock_map_latch_.lock();
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); ++iter) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      auto iso_level = txn->GetIsolationLevel();
      switch (iso_level) {
        case IsolationLevel::REPEATABLE_READ:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
            // LOG_DEBUG("txn:%d be set SHRINGKING", txn->GetTransactionId());
          }
          break;
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        default:
          UNREACHABLE("wrong IsolationLevel");
      }

      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);
      // LOG_DEBUG("txn:%d UnlockTable %d", txn->GetTransactionId(), oid);
      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();
      return true;
    }
  }

  ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, true);
}

auto LockManager::CheckAllRowsUnLock(Transaction *txn, const table_oid_t &oid) -> bool {
  return !((txn->GetExclusiveRowLockSet()->count(oid) != 0 && !(*txn->GetExclusiveRowLockSet())[oid].empty()) ||
           (txn->GetSharedRowLockSet()->count(oid) != 0 && !(*txn->GetSharedRowLockSet())[oid].empty()));
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW, true);
  }

  auto txn_state = txn->GetState();
  auto iso_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED) {
    return false;
  }
  switch (iso_level) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn_state == TransactionState::SHRINKING) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, true);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED) {
          ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, true);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_ON_SHRINKING, true);
      }
      if (lock_mode != LockMode::EXCLUSIVE) {
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED, true);
      }
      break;
    default:
      UNREACHABLE("wrong IsolationLevel");
  }

  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::TABLE_LOCK_NOT_PRESENT, true);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  txn_wait_map_[txn->GetTransactionId()] = lrq;
  row_lock_map_latch_.unlock();

  // 检查是否是一次锁升级(S->X)
  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) {  // 重复的锁
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {  // 抛出 UPGRADE_CONFLICT 异常
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::UPGRADE_CONFLICT, true);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {  // 抛 INCOMPATIBLE_UPGRADE 异常
        ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::INCOMPATIBLE_UPGRADE, true);
      }

      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(iter);
      RemoveFromTxnRowLockSet(txn, lr->lock_mode_, oid, rid);
      delete lr;
      lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
      upgrade = true;
      break;
    }
  }

  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
  }

  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
    // 死锁检测ABORT该事务 或者 手动ABORT该事务
    if (txn->GetState() == TransactionState::ABORTED) {
      // 移除该事务对该资源的request
      for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
        auto lr = *iter;
        if (lr->txn_id_ == txn->GetTransactionId()) {
          lrq->request_queue_.erase(iter);
          delete lr;
          break;
        }
      }
      lrq->cv_.notify_all();
      return false;
    }
  }
  AddIntoTxnRowLockSet(txn, lock_mode, oid, rid);
  // LOG_DEBUG("txn:%d LockRow oid:%d rid:%d:%d lock_mode:%d", txn->GetTransactionId(), oid, rid.GetPageId(),
  // rid.GetSlotNum(), lock_mode);
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  switch (row_lock_mode) {
    case LockMode::EXCLUSIVE:
      return txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
             txn->IsTableSharedIntentionExclusiveLocked(oid);
    case LockMode::SHARED:
      return txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
             txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
             txn->IsTableIntentionExclusiveLocked(oid);
    default:
      UNREACHABLE("wrong row lock mode");
  }
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); ++iter) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        auto iso_level = txn->GetIsolationLevel();
        switch (iso_level) {
          case IsolationLevel::REPEATABLE_READ:
            if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
              // LOG_DEBUG("txn:%d be set SHRINGKING", txn->GetTransactionId());
            }
            break;
          case IsolationLevel::READ_COMMITTED:
          case IsolationLevel::READ_UNCOMMITTED:
            if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
            break;
          default:
            UNREACHABLE("wrong IsolationLevel");
        }
      }
      RemoveFromTxnRowLockSet(txn, lr->lock_mode_, oid, rid);
      // LOG_DEBUG("txn:%d UnlockRow oid:%d rid:%d:%d", txn->GetTransactionId(), oid, rid.GetPageId(),
      // rid.GetSlotNum());
      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();
      return true;
    }
  }
  ABORT_FOR_REASON_DIRECTLY_OR_NOT(AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, true);
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::DFS(txn_id_t txn_id) -> bool {
  has_search_[txn_id] = true;
  stk_.push_back(txn_id);
  in_stk_[txn_id] = true;
  for (auto id : waits_for_[txn_id]) {
    if (!has_search_[id]) {
      return DFS(id);
    }
    if (in_stk_[txn_id]) {
      return true;
    }
  }
  stk_.pop_back();
  in_stk_[txn_id] = false;
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  return std::any_of(waits_for_.begin(), waits_for_.end(),
                     [this, txn_id](const std::pair<txn_id_t, std::set<txn_id_t>> &p) {
                       auto k = p.first;
                       if (!this->has_search_[k] && DFS(k)) {
                         *txn_id = this->stk_.back();
                         return true;
                       }
                       return false;
                     });
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[k, v] : waits_for_) {
    for (auto x : v) {
      edges.emplace_back(k, x);
    }
  }
  return edges;
}

void LockManager::BuildGraph() {
  table_lock_map_latch_.lock();
  row_lock_map_latch_.lock();

  for (auto &[k, v] : table_lock_map_) {
    std::vector<txn_id_t> granted;
    std::vector<txn_id_t> waited;
    {
      std::lock_guard<std::mutex> guard(v->latch_);
      for (auto lr : v->request_queue_) {
        if (txn_manager_->GetTransaction(lr->txn_id_) != nullptr &&
            txn_manager_->GetTransaction(lr->txn_id_)->GetState() != TransactionState::ABORTED) {
          if (lr->granted_) {
            granted.push_back(lr->txn_id_);
          } else {
            waited.push_back(lr->txn_id_);
          }
        }
      }
    }

    for (auto t2 : granted) {
      for (auto t1 : waited) {
        AddEdge(t1, t2);
      }
    }
  }

  for (auto &[k, v] : row_lock_map_) {
    std::vector<txn_id_t> granted;
    std::vector<txn_id_t> waited;
    {
      std::lock_guard<std::mutex> guard(v->latch_);
      for (auto lr : v->request_queue_) {
        if (txn_manager_->GetTransaction(lr->txn_id_) != nullptr &&
            txn_manager_->GetTransaction(lr->txn_id_)->GetState() != TransactionState::ABORTED) {
          if (lr->granted_) {
            granted.push_back(lr->txn_id_);
          } else {
            waited.push_back(lr->txn_id_);
          }
        }
      }
    }
    for (auto t2 : granted) {
      for (auto t1 : waited) {
        AddEdge(t1, t2);
      }
    }
  }
  row_lock_map_latch_.unlock();
  table_lock_map_latch_.unlock();
}

void LockManager::RemoveAllAboutAbortTxn(txn_id_t abort_id) {
  // 删除边
  waits_for_.erase(abort_id);

  for (auto iter = waits_for_.begin(); iter != waits_for_.end();) {
    if ((*iter).second.count(abort_id) != 0) {
      RemoveEdge((*iter).first, abort_id);
    }
    if ((*iter).second.empty()) {
      waits_for_.erase(iter++);
    } else {
      iter++;
    }
  }
}

void LockManager::PrintGraph() {
  for (auto &[k, v] : waits_for_) {
    std::cout << k << "-> ";
    for (auto x : v) {
      std::cout << x;
    }
    std::cout << std::endl;
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_.clear();
      BuildGraph();
      // PrintGraph();
      while (true) {
        stk_.clear();
        in_stk_.clear();
        has_search_.clear();
        txn_id_t abort_tid;
        if (HasCycle(&abort_tid)) {
          // LOG_DEBUG("abort_tid:%d", abort_tid);
          auto txn = txn_manager_->GetTransaction(abort_tid);
          txn->SetState(TransactionState::ABORTED);
          RemoveAllAboutAbortTxn(abort_tid);
          txn_wait_map_[abort_tid]->cv_.notify_all();
        } else {
          break;
        }
      }
    }
  }
}

void LockManager::AddIntoTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
  txn->UnlockTxn();
}

void LockManager::RemoveFromTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  txn->UnlockTxn();
}

void LockManager::AddIntoTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
  txn->UnlockTxn();
}

void LockManager::RemoveFromTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                          const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
  txn->UnlockTxn();
}

auto LockManager::AreLocksCompatible(LockMode mode1, LockMode mode2) -> bool {
  if (mode1 == LockMode::INTENTION_SHARED) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::INTENTION_EXCLUSIVE || mode2 == LockMode::SHARED ||
           mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (mode1 == LockMode::INTENTION_EXCLUSIVE) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::INTENTION_EXCLUSIVE;
  }
  if (mode1 == LockMode::SHARED) {
    return mode2 == LockMode::INTENTION_SHARED || mode2 == LockMode::SHARED;
  }
  if (mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return mode2 == LockMode::INTENTION_SHARED;
  }

  return false;
}
}  // namespace bustub
