//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    HashJoinKey key;
    for (auto &expression : plan_->RightJoinKeyExpressions()) {
      key.column_values_.emplace_back(expression->Evaluate(&right_tuple, right_child_->GetOutputSchema()));
    }
    // 加入哈希表
    if (map_.count(key) != 0) {
      map_[key].emplace_back(right_tuple);
    } else {
      map_[key] = {right_tuple};
    }
  }
  // 遍历左侧查询,得到查询结果
  Tuple left_tuple;
  RID left_rid;
  while (left_child_->Next(&left_tuple, &left_rid)) {
    HashJoinKey key;
    for (auto &expression : plan_->LeftJoinKeyExpressions()) {
      key.column_values_.emplace_back(expression->Evaluate(&left_tuple, left_child_->GetOutputSchema()));
    }
    if (map_.count(key) != 0) {  // 匹配成功
      for (auto &t : map_[key]) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (auto i = 0; i < static_cast<int>(left_child_->GetOutputSchema().GetColumnCount()); ++i) {
          values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (auto i = 0; i < static_cast<int>(right_child_->GetOutputSchema().GetColumnCount()); ++i) {
          values.emplace_back(t.GetValue(&right_child_->GetOutputSchema(), i));
        }
        result_.emplace_back(values, &GetOutputSchema());
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {  // 匹配失败,但是为LEFT JOIN
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (auto i = 0; i < static_cast<int>(left_child_->GetOutputSchema().GetColumnCount()); ++i) {
        values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (auto i = 0; i < static_cast<int>(right_child_->GetOutputSchema().GetColumnCount()); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      result_.emplace_back(values, &GetOutputSchema());
    }
  }

  index_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= static_cast<int>(result_.size())) {
    return false;
  }
  *tuple = result_[index_];
  index_++;
  return true;
}

}  // namespace bustub
