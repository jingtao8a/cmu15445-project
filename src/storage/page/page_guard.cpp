#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (page_ == that.page_) {
    return *this;
  }
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (guard_.page_ == that.guard_.page_) {
    return *this;
  }
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }
  guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  guard_.page_->RUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (guard_.page_ == that.guard_.page_) {
    return *this;
  }
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }
  guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  guard_.page_->WUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
