#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "common/macros.h"
#include "type/type.h"
namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    result_.emplace_back(tuple);
  }
  std::sort(result_.begin(), result_.end(),
            [this](const Tuple &left, const Tuple &right) -> bool { return this->TupleComparator(left, right); });
  index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= static_cast<int>(result_.size())) {
    return false;
  }
  *tuple = result_[index_];
  index_++;
  return true;
}

auto SortExecutor::TupleComparator(const Tuple &left, const Tuple &right) -> bool {
  auto &order_by = plan_->GetOrderBy();
  for (auto &p : order_by) {
    auto order = p.first;
    auto &exp = p.second;
    auto lvalue = exp->Evaluate(&left, child_executor_->GetOutputSchema());
    auto rvalue = exp->Evaluate(&right, child_executor_->GetOutputSchema());
    if (order == OrderByType::DESC) {
      if (lvalue.CompareGreaterThan(rvalue) == CmpBool::CmpTrue) {
        return true;
      }
      if (lvalue.CompareLessThan(rvalue) == CmpBool::CmpTrue) {
        return false;
      }
    } else {
      if (lvalue.CompareLessThan(rvalue) == CmpBool::CmpTrue) {
        return true;
      }
      if (lvalue.CompareGreaterThan(rvalue) == CmpBool::CmpTrue) {
        return false;
      }
    }
  }
  UNREACHABLE("duplicate key is not allowed");
}

}  // namespace bustub
