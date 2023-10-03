#include "execution/executors/topn_executor.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    HeapKeyType key(tuple, plan_->GetOrderBy(), child_executor_.get());
    heap_.emplace(tuple, plan_->GetOrderBy(), child_executor_.get());
    if (heap_.size() > plan_->GetN()) {
      heap_.pop();
    }
  }

  while (!heap_.empty()) {
    result_.emplace_front(heap_.top().tuple_);
    heap_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (GetNumInHeap() != 0) {
    *tuple = result_.front();
    result_.pop_front();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return result_.size(); };

}  // namespace bustub
