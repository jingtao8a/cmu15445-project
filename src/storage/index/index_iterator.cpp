/**
 * index_iterator.cpp
 */
#include <cassert>
#include <optional>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return read_guard_ == std::nullopt; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto page = read_guard_->As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  BUSTUB_ASSERT(page->GetSize() > index_, "index_ must be valid");
  return page->GetArray()[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  auto leaf_page = read_guard_->As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (index_ + 1 < leaf_page->GetSize()) {
    index_++;
    return *this;
  }
  if (leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
    read_guard_ = bpm_->FetchPageRead(leaf_page->GetNextPageId());
    index_ = 0;
    return *this;
  }
  read_guard_ = std::nullopt;
  index_ = INVALID_PAGE_ID;
  return *this;
};

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
