//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey key = MakeAggregateKey(&tuple);
    AggregateValue value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_.Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if ((*aht_iterator_) == aht_.End()) {
    return false;
  }
  auto key = aht_iterator_->Key();
  auto value = aht_iterator_->Val();
  ++(*aht_iterator_);
  key.group_bys_.insert(key.group_bys_.end(), value.aggregates_.begin(), value.aggregates_.end());
  *tuple = {key.group_bys_, &GetOutputSchema()};
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
