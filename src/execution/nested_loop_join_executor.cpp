//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.emplace_back(right_tuple);
  }
  index_ = 0;
  is_match_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  if (index_ != 0) {
    // 上次右侧循环还未结束
    if (NestedLoop(tuple, rid)) {
      return true;
    }
  }

  while (left_executor_->Next(&left_tuple_, &left_rid)) {
    right_executor_->Init();  // no use
    if (NestedLoop(tuple, rid)) {
      return true;
    }
  }
  return false;
}

auto NestedLoopJoinExecutor::NestedLoop(Tuple *tuple, RID *rid) -> bool {
  while (index_ < static_cast<int>(right_tuples_.size())) {
    if (plan_->predicate_) {
      auto res = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                 &right_tuples_[index_], right_executor_->GetOutputSchema());
      if (!(!res.IsNull() && res.GetAs<bool>())) {  // 不符合条件
        index_++;
        continue;  // 过滤
      }
    }
    // 符合条件
    MergeTuple(tuple);
    index_ = (index_ + 1) % right_tuples_.size();
    is_match_ = (index_ != 0);
    return true;
  }

  index_ = 0;
  if (!is_match_ && plan_->GetJoinType() == JoinType::LEFT) {
    // left join
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto i = 0; i < static_cast<int>(left_executor_->GetOutputSchema().GetColumnCount()); ++i) {
      values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (auto i = 0; i < static_cast<int>(right_executor_->GetOutputSchema().GetColumnCount()); ++i) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    *tuple = {values, &GetOutputSchema()};
    return true;
  }

  is_match_ = false;
  return false;
}

void NestedLoopJoinExecutor::MergeTuple(Tuple *tuple) {
  // inner join
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (auto i = 0; i < static_cast<int>(left_executor_->GetOutputSchema().GetColumnCount()); ++i) {
    values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (auto i = 0; i < static_cast<int>(right_executor_->GetOutputSchema().GetColumnCount()); ++i) {
    values.emplace_back(right_tuples_[index_].GetValue(&right_executor_->GetOutputSchema(), i));
  }
  *tuple = {values, &GetOutputSchema()};
}

}  // namespace bustub
