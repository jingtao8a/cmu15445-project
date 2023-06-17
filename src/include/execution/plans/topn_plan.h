//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_plan.h
//
// Identification: src/include/execution/plans/topn_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bits/utility.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNPlanNode represents a top-n operation. It will gather the n extreme rows based on
 * limit and order expressions.
 */
class TopNPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new TopNPlanNode instance.
   * @param output The output schema of this topN plan node
   * @param child The child plan node
   * @param order_bys The sort expressions and their order by types.
   * @param n Retain n elements.
   */
  TopNPlanNode(SchemaRef output, AbstractPlanNodeRef child,
               std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, std::size_t n)
      : AbstractPlanNode(std::move(output), {std::move(child)}), order_bys_(std::move(order_bys)), n_{n} {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::TopN; }

  /** @return The N (limit) */
  auto GetN() const -> size_t { return n_; }

  /** @return Get order by expressions */
  auto GetOrderBy() const -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & { return order_bys_; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "TopN should have exactly one child plan.");
    return GetChildAt(0);
  }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(TopNPlanNode);

  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  std::size_t n_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

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
