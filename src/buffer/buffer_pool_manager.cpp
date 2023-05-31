//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstdlib>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t free_frame_id = -1;
  std::lock_guard<std::mutex> guard(latch_);
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&free_frame_id)) {
      return nullptr;
    }
    if (pages_[free_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[free_frame_id].page_id_, pages_[free_frame_id].data_);
    }
    page_table_.erase(pages_[free_frame_id].page_id_);
    pages_[free_frame_id].ResetMemory();
  }
  *page_id = AllocatePage();
  pages_[free_frame_id].page_id_ = *page_id;
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;
  page_table_[*page_id] = free_frame_id;

  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);  // no use
  return pages_ + free_frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is equal to INVALID_PAGE_ID");
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) != 0) {
    pages_[page_table_[page_id]].pin_count_++;
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    return pages_ + page_table_[page_id];
  }

  frame_id_t free_frame_id = -1;
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&free_frame_id)) {
      return nullptr;
    }
    if (pages_[free_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[free_frame_id].page_id_, pages_[free_frame_id].data_);
    }
    page_table_.erase(pages_[free_frame_id].page_id_);
    pages_[free_frame_id].ResetMemory();
  }

  pages_[free_frame_id].page_id_ = page_id;
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;
  page_table_[page_id] = free_frame_id;
  disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);

  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);  // no use
  return pages_ + free_frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) <= 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) <= 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unordered_map<page_id_t, frame_id_t> back_page_table;
  {
    std::lock_guard<std::mutex> guard(latch_);
    back_page_table = page_table_;
  }

  for (auto &p : back_page_table) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) <= 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
