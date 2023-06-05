#include <cstddef>
#include <optional>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/expressions/comparison_expression.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  {
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    ctx.root_page_id_ = header_page->root_page_id_;
    ctx.read_set_.push_back(bpm_->FetchPageRead(ctx.root_page_id_));
  }
  FindLeafPage(key, Operation::Search, ctx);
  auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  int index = -1;
  if (leaf_page->KeyIndex(key, comparator_, index)) {
    result->push_back(leaf_page->ValueAt(index));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation op, Context &ctx) {
  if (op == Operation::Search) {
    auto page = ctx.read_set_.back().As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal = ctx.read_set_.back().As<InternalPage>();
      auto next_page_id = internal->LookUp(key, comparator_);
      ctx.read_set_.push_back(bpm_->FetchPageRead(next_page_id));
      ctx.read_set_.pop_front();
      page = ctx.read_set_.back().As<BPlusTreePage>();
    }
    return;
  }
  if (op == Operation::Insert || op == Operation::Remove) {
    auto page = ctx.write_set_.back().As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal = ctx.write_set_.back().As<InternalPage>();
      auto next_page_id = internal->LookUp(key, comparator_);
      ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
      if (IsSafePage(ctx.write_set_.back().As<BPlusTreePage>(), op, false)) {
        while (ctx.write_set_.size() > 1) {
          ctx.write_set_.pop_front();
        }
      }
      page = ctx.write_set_.back().As<BPlusTreePage>();
    }
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafePage(const BPlusTreePage *tree_page, Operation op, bool isRootPage) -> bool {
  if (op == Operation::Search) {
    return true;
  }
  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() + 1 < tree_page->GetMaxSize();
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }
  if (op == Operation::Remove) {
    if (isRootPage) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    return tree_page->GetSize() > tree_page->GetMinSize();
  }
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn)
    -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // root not exist,start a new tree
    auto root_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    header_page->root_page_id_ = ctx.root_page_id_;
    auto leaf_page = root_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_, 1);
    leaf_page->GetArray()[0] = {key, value};
    ctx.Drop();
    return true;
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  if (IsSafePage(ctx.write_set_.back().As<BPlusTreePage>(), Operation::Insert, true)) {
    ctx.header_page_ = std::nullopt;  // unlock header_page
  }
  FindLeafPage(key, Operation::Insert, ctx);
  auto &leaf_page_guard = ctx.write_set_.back();
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
  if (!leaf_page->Insert(key, value, comparator_)) {  // duplicate key, 插入失败
    ctx.Drop();
    return false;
  }
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // 叶子节点未溢出，不需要分裂
    ctx.Drop();
    return true;
  }
  // 发生溢出,叶子节点分裂
  auto new_page_id = 0;
  auto new_leaf_page_guard = bpm_->NewPageGuarded(&new_page_id);
  auto new_leaf_page = new_leaf_page_guard.AsMut<LeafPage>();
  std::copy(leaf_page->GetArray() + leaf_page->GetMinSize(), leaf_page->GetArray() + leaf_page->GetSize(),
            new_leaf_page->GetArray());
  new_leaf_page->Init(leaf_max_size_, leaf_page->GetSize() - leaf_page->GetMinSize(), leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_guard.PageId());
  leaf_page->SetSize(leaf_page->GetMinSize());
  KeyType split_key = new_leaf_page->KeyAt(0);
  // 将split_key插入父节点
  InsertIntoParent(split_key, new_leaf_page_guard.PageId(), ctx, ctx.write_set_.size() - 2);
  ctx.Drop();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const KeyType &key, page_id_t right_child_id, Context &ctx, int index) {
  if (index < 0) {  // parent为header_page
    auto new_root_page_id = 0;
    auto new_root_page_guard = bpm_->NewPageGuarded(&new_root_page_id);
    auto new_root_page = new_root_page_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_, 2);
    new_root_page->GetArray()[0].second = ctx.write_set_[index + 1].PageId();
    new_root_page->GetArray()[1] = {key, right_child_id};
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;
    return;
  }
  auto parent_page = ctx.write_set_[index].AsMut<InternalPage>();
  if (parent_page->Insert(key, right_child_id, comparator_)) {  // 父节点不需要分裂
    return;
  }
  // 父节点需要分裂
  auto new_parent_page_id = 0;
  auto new_parent_page_guard = bpm_->NewPageGuarded(&new_parent_page_id);
  auto new_parent_page = new_parent_page_guard.AsMut<InternalPage>();
  auto array = new std::pair<KeyType, page_id_t>[parent_page->GetMaxSize() + 1];
  std::copy(parent_page->GetArray(), parent_page->GetArray() + parent_page->GetMaxSize(), array);
  // upper_bound
  int l = 1;
  int r = parent_page->GetMaxSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator_(array[mid].first, key) > 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  // 右移一位，腾出空间
  for (int i = parent_page->GetMaxSize() - 1; i >= l; --i) {
    array[i + 1] = array[i];
  }
  array[l] = {key, right_child_id};
  std::copy(array, array + parent_page->GetMinSize(), parent_page->GetArray());
  std::copy(array + parent_page->GetMinSize(), array + parent_page->GetMaxSize() + 1, new_parent_page->GetArray());
  new_parent_page->Init(internal_max_size_, parent_page->GetMaxSize() + 1 - parent_page->GetMinSize());
  parent_page->SetSize(parent_page->GetMinSize());
  delete[] array;
  InsertIntoParent(new_parent_page->KeyAt(0), new_parent_page_id, ctx, index - 1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // root not exist
    return;
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  if (IsSafePage(ctx.write_set_.back().As<BPlusTreePage>(), Operation::Remove, true)) {
    ctx.header_page_ = std::nullopt;  // unlock header_page
  }
  FindLeafPage(key, Operation::Remove, ctx);
  auto &leaf_page_guard = ctx.write_set_.back();
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
  int pos = -1;
  // key不存在
  if (!leaf_page->KeyIndex(key, comparator_, pos)) {
    ctx.Drop();
    return;
  }
  // key存在,将其从leaf中删除
  for (int i = pos + 1; i < leaf_page->GetSize(); ++i) {
    leaf_page->GetArray()[i - 1] = leaf_page->GetArray()[i];
  }
  leaf_page->SetSize(leaf_page->GetSize() - 1);  // 更新leaf_page的size

  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {  // 无underflow 直接返回
    ctx.Drop();
    return;
  }
  // underflow
  if (ctx.IsRootPage(leaf_page_guard.PageId())) {  // 该叶子节点是根节点
    if (leaf_page->GetSize() == 0) {               // size为0
      header_page->root_page_id_ = INVALID_PAGE_ID;
    }
    ctx.Drop();
    return;
  }

  auto &parent_page_guard = ctx.write_set_[ctx.write_set_.size() - 2];
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  auto index = parent_page->ValueIndex(leaf_page_guard.PageId());
  BUSTUB_ASSERT(index != -1, "index must not be -1");
  // 如果有右brother
  if (index < parent_page->GetSize() - 1) {
    page_id_t right_brother_page_id = parent_page->GetArray()[index + 1].second;
    auto right_brother_page_guard = bpm_->FetchPageWrite(right_brother_page_id);
    auto right_brother_page = right_brother_page_guard.AsMut<LeafPage>();

    auto merge_size = right_brother_page->GetSize() + leaf_page->GetSize();
    if (merge_size < leaf_page->GetMaxSize()) {  // 可以合并
      // merge
      std::copy(right_brother_page->GetArray(), right_brother_page->GetArray() + right_brother_page->GetSize(),
                leaf_page->GetArray() + leaf_page->GetSize());
      leaf_page->SetSize(merge_size);
      leaf_page->SetNextPageId(right_brother_page->GetNextPageId());
      RemoveFromParent(index + 1, ctx, ctx.write_set_.size() - 2);
    } else {
      // borrow
      leaf_page->GetArray()[leaf_page->GetSize()] = right_brother_page->GetArray()[0];
      std::copy(right_brother_page->GetArray() + 1, right_brother_page->GetArray() + right_brother_page->GetSize(),
                right_brother_page->GetArray());
      leaf_page->IncreaseSize(1);
      right_brother_page->SetSize(right_brother_page->GetSize() - 1);
      parent_page->SetKeyAt(index + 1, right_brother_page->GetArray()[0].first);
    }
  } else {
    // 左brother
    BUSTUB_ASSERT(index - 1 >= 0, "left brother must exist");
    page_id_t left_brother_page_id = parent_page->GetArray()[index - 1].second;
    auto left_brother_page_guard = bpm_->FetchPageWrite(left_brother_page_id);
    auto left_brother_page = left_brother_page_guard.AsMut<LeafPage>();

    auto merge_size = left_brother_page->GetSize() + leaf_page->GetSize();
    if (merge_size < left_brother_page->GetMaxSize()) {  // 可以合并
      // merge
      std::copy(leaf_page->GetArray(), leaf_page->GetArray() + leaf_page->GetSize(),
                left_brother_page->GetArray() + left_brother_page->GetSize());
      left_brother_page->SetSize(merge_size);
      left_brother_page->SetNextPageId(leaf_page->GetNextPageId());
      RemoveFromParent(index, ctx, ctx.write_set_.size() - 2);
    } else {
      // borrow
      for (int i = leaf_page->GetSize(); i >= 1; --i) {
        leaf_page->GetArray()[i] = leaf_page->GetArray()[i - 1];
      }
      leaf_page->GetArray()[0] = left_brother_page->GetArray()[left_brother_page->GetSize() - 1];
      leaf_page->IncreaseSize(1);
      left_brother_page->SetSize(left_brother_page->GetSize() - 1);
      parent_page->SetKeyAt(index, leaf_page->GetArray()[0].first);
    }
  }
  ctx.Drop();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromParent(int valueIndex, Context &ctx, int index) {
  auto &page_guard = ctx.write_set_[index];
  auto page = page_guard.AsMut<InternalPage>();
  for (int i = valueIndex + 1; i < page->GetSize(); ++i) {  // 删除key value
    page->GetArray()[i - 1] = page->GetArray()[i];
  }
  page->SetSize(page->GetSize() - 1);  // 更新page的size

  if (page->GetSize() >= page->GetMinSize()) {  // 无underflow
    return;
  }
  // underflow
  if (ctx.IsRootPage(page_guard.PageId())) {  // 该page是根节点
    if (page->GetSize() == 1) {               // 根节点需要更换了
      BUSTUB_ASSERT(ctx.header_page_ != std::nullopt, "ctx.header_page must exist");
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = page->GetArray()[0].second;
    }
    return;
  }
  BUSTUB_ASSERT(index - 1 >= 0, "parent_page_guard must exist");
  auto &parent_page_guard = ctx.write_set_[index - 1];
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  auto pos = parent_page->ValueIndex(page_guard.PageId());
  BUSTUB_ASSERT(pos != -1, "pos must not be -1");
  // 如果有右brother
  if (pos < parent_page->GetSize() - 1) {
    page_id_t right_brother_page_id = parent_page->GetArray()[pos + 1].second;
    auto right_brother_page_guard = bpm_->FetchPageWrite(right_brother_page_id);
    auto right_brother_page = right_brother_page_guard.AsMut<InternalPage>();

    auto merge_size = right_brother_page->GetSize() + page->GetSize();
    if (merge_size <= page->GetMaxSize()) {  // 可以合并
      // merge
      std::copy(right_brother_page->GetArray(), right_brother_page->GetArray() + right_brother_page->GetSize(),
                page->GetArray() + page->GetSize());
      page->SetSize(merge_size);
      RemoveFromParent(pos + 1, ctx, index - 1);
    } else {
      // borrow
      page->GetArray()[page->GetSize()] = right_brother_page->GetArray()[0];
      std::copy(right_brother_page->GetArray() + 1, right_brother_page->GetArray() + right_brother_page->GetSize(),
                right_brother_page->GetArray());
      page->IncreaseSize(1);
      right_brother_page->SetSize(right_brother_page->GetSize() - 1);
      parent_page->SetKeyAt(pos + 1, right_brother_page->GetArray()[0].first);
    }
  } else {
    // 左brother
    BUSTUB_ASSERT(pos - 1 >= 0, "left brother must exist");
    page_id_t left_brother_page_id = parent_page->GetArray()[pos - 1].second;
    auto left_brother_page_guard = bpm_->FetchPageWrite(left_brother_page_id);
    auto left_brother_page = left_brother_page_guard.AsMut<InternalPage>();

    auto merge_size = left_brother_page->GetSize() + page->GetSize();
    if (merge_size <= left_brother_page->GetMaxSize()) {  // 可以合并
      // merge
      std::copy(page->GetArray(), page->GetArray() + page->GetSize(),
                left_brother_page->GetArray() + left_brother_page->GetSize());
      left_brother_page->SetSize(merge_size);
      RemoveFromParent(pos, ctx, index - 1);
    } else {
      // borrow
      for (int i = page->GetSize(); i >= 1; --i) {
        page->GetArray()[i] = page->GetArray()[i - 1];
      }
      page->GetArray()[0] = left_brother_page->GetArray()[left_brother_page->GetSize() - 1];
      page->IncreaseSize(1);
      left_brother_page->SetSize(left_brother_page->GetSize() - 1);
      parent_page->SetKeyAt(pos, page->GetArray()[0].first);
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw Exception("BplusTree is empty");
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->FetchPageRead(ctx.root_page_id_));
  header_page_guard.Drop();

  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal = ctx.read_set_.back().As<InternalPage>();
    page_id_t id = internal->ValueAt(0);
    ctx.read_set_.push_back(bpm_->FetchPageRead(id));
    ctx.read_set_.pop_front();
    page = ctx.read_set_.back().As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, std::move(ctx.read_set_.back()));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    throw Exception("BplusTree is empty");
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->FetchPageRead(ctx.root_page_id_));
  header_page_guard.Drop();
  FindLeafPage(key, Operation::Search, ctx);
  int pos = -1;
  if (!ctx.read_set_.back().As<LeafPage>()->KeyIndex(key, comparator_, pos)) {
    throw Exception("key not exist");
  }
  return INDEXITERATOR_TYPE(bpm_, std::move(ctx.read_set_.back()), pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto page = guard.As<BPlusTreeHeaderPage>();
  return page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
