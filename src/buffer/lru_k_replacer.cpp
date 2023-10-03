//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cmath>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  *frame_id = -1;
  for (auto &p : node_store_) {
    if (p.second.is_evictable_) {
      if (*frame_id == -1 || Judge(p.second, node_store_[*frame_id])) {
        *frame_id = p.second.fid_;
      }
    }
  }
  if (*frame_id != -1) {
    node_store_.erase(*frame_id);
    --curr_size_;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    throw Exception("frame_id is larger than or equal to replacer_size_");
  }
  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode();
    node_store_[frame_id].fid_ = frame_id;
  }
  auto &node = node_store_[frame_id];
  node.history_.push_front(current_timestamp_++);
  while (node.history_.size() > k_) {
    node.history_.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (node_store_.count(frame_id) <= 0) {
    throw Exception("frame_id should be used");
  }
  if (!node_store_[frame_id].is_evictable_ && set_evictable) {  // false -> true
    curr_size_++;
  } else if (node_store_[frame_id].is_evictable_ && !set_evictable) {  // true -> false
    curr_size_--;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (node_store_.count(frame_id) <= 0) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    throw Exception("Remove a non-evictable frame");
  }
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
