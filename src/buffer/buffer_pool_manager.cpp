//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

using namespace std;

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  lock_guard<mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  // step 1.1
  if (iter != page_table_.end()) {
    auto page = pages_ + iter->second;
    if (page->pin_count_ > 0) {
      return page;
    }
    replacer_->Pin(iter->second);
    return page;
  }
  // step 1.2, 2, and 3
  auto frame_id = PickVictimFrame();
  if (frame_id < 0) {
    return nullptr;
  }
  auto page = pages_ + frame_id;
  // step 3
  page_table_[page_id] = frame_id;
  // step 4
  page->page_id_ = page_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto page = pages_ + page_table_[page_id];
  if (--page->pin_count_ <= 0) {
    replacer_->Unpin(iter->second);
  }
  page->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  lock_guard<mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto page = pages_ + page_table_[page_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  lock_guard<mutex> guard(latch_);
  // step 1
  if (AllPagesPinned()) {
    return nullptr;
  }
  // step 2
  auto frame_id = PickVictimFrame();
  if (frame_id < 0) {
    return nullptr;
  }
  auto page = pages_ + frame_id;
  // step 3 and 4
  *page_id = disk_manager_->AllocatePage();
  page_table_[*page_id] = frame_id;
  page->page_id_ = *page_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  lock_guard<mutex> guard(latch_);
  // step 1
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto page = pages_ + page_table_[page_id];
  // step 2
  if (page->pin_count_ > 0) {
    return false;
  }
  // step 3
  free_list_.push_back(iter->second);
  page_table_.erase(iter);
  page->ResetMemory();
  // step 0
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

/** helper functions */

frame_id_t BufferPoolManager::PickVictimFrame() {
  frame_id_t frame_id;
  // pick from free list first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }
  if (!replacer_->Victim(&frame_id)) {
    return -1;
  }
  // remove victim page from buffer pool
  auto page = pages_ + frame_id;
  auto page_id = page->GetPageId();
  page_table_.erase(page_id);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  return frame_id;
}

bool BufferPoolManager::AllPagesPinned() {
  for (size_t i = 0; i < pool_size_; ++i) {
    auto page = pages_ + i;
    if (page->pin_count_ <= 0) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
