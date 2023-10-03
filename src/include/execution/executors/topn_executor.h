//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HeapKeyType {
  HeapKeyType(Tuple tuple, const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
              AbstractExecutor *child_executor)
      : tuple_(std::move(tuple)), order_by_(order_by), child_executor_(child_executor) {}

  HeapKeyType(const HeapKeyType &rhs) : order_by_(rhs.order_by_) {
    tuple_ = rhs.tuple_;
    child_executor_ = rhs.child_executor_;
  }

  HeapKeyType(HeapKeyType &&rhs) noexcept : order_by_(rhs.order_by_) {
    tuple_ = std::move(rhs.tuple_);
    child_executor_ = rhs.child_executor_;
  }

  auto operator=(const HeapKeyType &rhs) -> HeapKeyType & {
    tuple_ = rhs.tuple_;
    return *this;
  }

  auto operator=(HeapKeyType &&rhs) noexcept -> HeapKeyType & {
    tuple_ = std::move(rhs.tuple_);
    return *this;
  }

  Tuple tuple_;
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_;
  AbstractExecutor *child_executor_;
};

}  // namespace bustub

namespace std {
template <>
struct less<bustub::HeapKeyType> {
  auto operator()(const bustub::HeapKeyType &lhs, const bustub::HeapKeyType &rhs) const -> bool {
    auto child_executor = lhs.child_executor_;
    for (auto &p : lhs.order_by_) {
      auto order = p.first;
      auto &exp = p.second;
      auto lvalue = exp->Evaluate(&lhs.tuple_, child_executor->GetOutputSchema());
      auto rvalue = exp->Evaluate(&rhs.tuple_, child_executor->GetOutputSchema());
      if (order == bustub::OrderByType::DESC) {
        if (lvalue.CompareGreaterThan(rvalue) == bustub::CmpBool::CmpTrue) {
          return true;
        }
        if (lvalue.CompareLessThan(rvalue) == bustub::CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (lvalue.CompareLessThan(rvalue) == bustub::CmpBool::CmpTrue) {
          return true;
        }
        if (lvalue.CompareGreaterThan(rvalue) == bustub::CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    UNREACHABLE("duplicate key is not allowed");
  }
};
}  // namespace std

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::priority_queue<HeapKeyType> heap_;
  std::deque<Tuple> result_;
};
}  // namespace bustub
