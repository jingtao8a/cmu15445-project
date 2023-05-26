#include "primer/trie.h"
#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!root_) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> ptr(root_);
  for (char ch : key) {
    if (ptr->children_.count(ch) == 0) {
      return nullptr;
    }
    ptr = ptr->children_.at(ch);
  }
  if (!ptr->is_value_node_) {
    return nullptr;
  }
  auto p = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(ptr);
  if (!p) {
    return nullptr;
  }
  return p->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<const TrieNode> new_root(nullptr);
  std::map<char, std::shared_ptr<const TrieNode>> children;
  if (key.length() == 0) {
    if (root_) {
      children = root_->children_;
    }
    new_root = std::make_shared<const TrieNodeWithValue<T>>(children, std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }

  std::vector<std::unique_ptr<TrieNode>> stack;
  if (root_) {
    stack.push_back(root_->Clone());
  } else {
    stack.push_back(std::make_unique<TrieNode>());
  }
  auto ptr(root_);

  for (int64_t i = 0; i < static_cast<int64_t>(key.length() - 1); ++i) {
    std::unique_ptr<TrieNode> tmp_ptr(nullptr);
    if (ptr && ptr->children_.count(key[i]) == 1) {
      ptr = ptr->children_.at(key[i]);
      tmp_ptr = ptr->Clone();
    } else {
      tmp_ptr = std::make_unique<TrieNode>();
      ptr = nullptr;
    }

    stack.push_back(std::move(tmp_ptr));
  }
  auto value_ptr = std::make_shared<T>(std::move(value));
  if (ptr && ptr->children_.count(key.back())) {
    ptr = ptr->children_.at(key.back());
    children = ptr->children_;
  }
  auto value_node = std::make_unique<TrieNodeWithValue<T>>(children, std::move(value_ptr));
  stack.push_back(std::move(value_node));

  for (int64_t i = key.length() - 1; i >= 0; i--) {
    auto tmp_ptr = std::move(stack.back());
    stack.pop_back();
    stack.back()->children_[key[i]] = std::move(tmp_ptr);
  }
  new_root = std::move(stack.back());
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<TrieNode> new_root(nullptr);

  std::vector<std::unique_ptr<TrieNode>> stack;
  if (root_) {
    stack.push_back(root_->Clone());
  } else {
    return {};
  }
  auto ptr = root_;

  for (int64_t i = 0; i < static_cast<int64_t>(key.length() - 1); ++i) {
    if (ptr->children_.count(key[i]) == 0) {
      return Trie(root_);
    }
    ptr = ptr->children_.at(key[i]);
    stack.push_back(ptr->Clone());
  }
  if (ptr->children_.count(key.back()) == 0) {
    return Trie(root_);
  }
  ptr = ptr->children_.at(key.back());
  if (!ptr->is_value_node_) {
    return Trie(root_);
  }
  auto children = ptr->children_;
  stack.push_back(std::make_unique<TrieNode>(children));

  for (int64_t i = key.length() - 1; i >= 0; --i) {
    auto tmp_ptr = std::move(stack.back());
    stack.pop_back();
    if (tmp_ptr->children_.empty() && !tmp_ptr->is_value_node_) {
      stack.back()->children_.erase(key[i]);
    } else {
      stack.back()->children_[key[i]] = std::move(tmp_ptr);
    }
  }
  new_root = std::move(stack.front());
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    return {};
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
