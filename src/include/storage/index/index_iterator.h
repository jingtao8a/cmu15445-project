//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <optional>
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard read_guard)
      : read_guard_(std::move(read_guard)), index_(0), bpm_(bpm) {}
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard read_guard, int index)
      : read_guard_(std::move(read_guard)), index_(index), bpm_(bpm) {}

  IndexIterator(const IndexIterator &) = default;
  IndexIterator(IndexIterator &&) noexcept = default;

  ~IndexIterator();  // NOLINT

  auto IsEnd() const -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (IsEnd() && itr.IsEnd()) {
      return true;
    }
    if (IsEnd() || itr.IsEnd()) {
      return false;
    }
    return read_guard_->PageId() == itr.read_guard_->PageId() && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  mutable std::optional<ReadPageGuard> read_guard_{std::nullopt};
  int index_{INVALID_PAGE_ID};
  BufferPoolManager *bpm_{nullptr};
};

}  // namespace bustub
