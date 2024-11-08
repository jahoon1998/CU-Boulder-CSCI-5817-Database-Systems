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
#include "common/logger.h"

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
  // page_id exists in the table

  if (page_table_.find(page_id) != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_ += 1;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_to_evict;
  if (!free_list_.empty()) {
    frame_to_evict = free_list_.front();
    free_list_.pop_front();
  } else {
    // if free _list is empty, get a victim page P from the replace
    if (!replacer_->Victim(&frame_to_evict)) {
      // if a victim is not found from the replacer, frame_id is set to -1;
      frame_to_evict = -1;
      return nullptr;
    }
    if (pages_[frame_to_evict].IsDirty()) {
      // 2.     If R is dirty, write it back to the disk.
      disk_manager_->WritePage(pages_[frame_to_evict].GetPageId(), pages_[frame_to_evict].GetData());
    }
  }
  // 3.     Delete R from the page table and insert P.

  if (page_table_.find(pages_[frame_to_evict].GetPageId()) != page_table_.end()) {
    page_table_.erase(pages_[frame_to_evict].GetPageId());
  }

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page_table_[page_id] = frame_to_evict;

  pages_[frame_to_evict].ResetMemory();
  pages_[frame_to_evict].page_id_ = page_id;
  pages_[frame_to_evict].pin_count_ = 1;
  pages_[frame_to_evict].is_dirty_ = false;
  replacer_->Pin(frame_to_evict);

  disk_manager_->ReadPage(page_id, pages_[frame_to_evict].data_);
  Page *result = &pages_[frame_to_evict];

  return result;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) {
    // page_id doesn't exist
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  if (!pages_[frame_id].IsDirty()) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // page_id exists in the table
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  *page_id = INVALID_PAGE_ID;

  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // bool all_pinned = true;
  // if (sizeof(pages_) >= pool_size_) {
  //   for (unsigned int i = 0; i < sizeof(pages_); i = i + 1) {
  //     if (pages_[i].GetPinCount() == 0) {
  //       all_pinned = false;
  //     }
  //   }
  // }
  // if (all_pinned) {
  //   return nullptr;
  // }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // if free_list_ is not empty, pick a frame from it
  frame_id_t frame_to_evict;
  if (!free_list_.empty()) {
    frame_to_evict = free_list_.front();
    free_list_.pop_front();
  } else {
    // if free _list is empty, get a victim page P from the replace
    if (!replacer_->Victim(&frame_to_evict)) {
      // if a victim is not found from the replacer, frame_id is set to -1;

      return nullptr;
    }
    // if frame_to_evict has a dirty bit, write to the disk
    if (pages_[frame_to_evict].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_to_evict].GetPageId(), pages_[frame_to_evict].GetData());
    }
  }
  // Make sure you call DiskManager::AllocatePage!
  page_id_t new_page_id = disk_manager_->AllocatePage();
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  if (page_table_.find(pages_[frame_to_evict].GetPageId()) != page_table_.end()) {
    page_table_.erase(pages_[frame_to_evict].GetPageId());
  }
  page_table_[new_page_id] = frame_to_evict;

  pages_[frame_to_evict].ResetMemory();
  pages_[frame_to_evict].page_id_ = new_page_id;
  pages_[frame_to_evict].pin_count_ = 1;
  pages_[frame_to_evict].is_dirty_ = false;
  replacer_->Pin(frame_to_evict);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  Page *result = &pages_[frame_to_evict];

  return result;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame = page_table_[page_id];
  if (pages_[frame].GetPinCount() > 0) {
    return false;
  }
  if (pages_[frame].IsDirty()) {
    disk_manager_->WritePage(pages_[frame].GetPageId(), pages_[frame].GetData());
  }
  page_table_.erase(pages_[frame].GetPageId());

  pages_[frame].ResetMemory();
  pages_[frame].page_id_ = INVALID_PAGE_ID;
  pages_[frame].pin_count_ = 0;
  pages_[frame].is_dirty_ = false;

  // remove frame from LRU list
  replacer_->Pin(frame);
  free_list_.push_back(frame);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!

  for (size_t i = 0; i < sizeof(pages_); i++) {
    page_id_t page_id = pages_[i].GetPageId();
    this->FlushPageImpl(page_id);
  }
}

}  // namespace bustub
