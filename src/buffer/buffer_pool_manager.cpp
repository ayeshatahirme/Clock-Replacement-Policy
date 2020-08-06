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

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

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

  latch_.lock();
  frame_id_t frameID = -1;
  Page *replaced;

  // If it finds the page of given ID in table, it will get and return that page
  if (page_table_.find(page_id) != page_table_.end()) {
    frameID = page_table_[page_id];
	// it will pin the frame ID 
    replacer_->Pin(frameID);
    pages_[frameID].pin_count_ += 1;
    latch_.unlock();
    return &pages_[frameID];
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frameID = free_list_.front();
    free_list_.pop_front();
  } 
  else {
    if (!replacer_->Victim(&frameID)) {
      latch_.unlock();
	  // in case no slot is available we will return null pointer 
      return nullptr;
    }
  
	// 2.     If R is dirty, write it back to the disk.
    replaced = &pages_[frameID];
    if (replaced->is_dirty_) {
      FlushPageImpl(replaced->page_id_);
      replaced->is_dirty_ = false;
    }

    // 3.     Delete R from the page table and insert P.
    // in case R is dirty we will delete its entry and the new page P is inserted
	page_table_.erase(replaced->page_id_);
  }
  pages_[frameID].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frameID].GetData());

  // 4  Update P's metadata, and then return a pointer to P.
  // after deletion of page its metadata information is also updated
  pages_[frameID].page_id_ = page_id;
  page_table_.insert({page_id, frameID});
  pages_[frameID].pin_count_ = 1;
  replacer_->Pin(frameID);

  latch_.unlock();
  // the page's pointer is returned
  return &pages_[frameID];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  Page *unpin_p;

  // the pin counter of page is decremented
  // if page is unpinned, it can be changed
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  unpin_p = &pages_[page_table_[page_id]];
  if (unpin_p->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  unpin_p->pin_count_ -= 1;
  unpin_p->is_dirty_ = is_dirty;
  // if counter is 0, it will be added in replacer
  if (unpin_p->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  Page *flush_P;
  
	// it will check page in table if found then the
  // changes are sent back on secondary storage
  // data is written back on the disk
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  flush_P = &pages_[page_table_[page_id]];
  if (flush_P->is_dirty_) {
    disk_manager_->WritePage(flush_P->page_id_, flush_P->GetData());
    flush_P->is_dirty_ = false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  
  // in this function we directly fetch pages from disk manager
  latch_.lock();
  frame_id_t frameID = -1;
  Page *replace_P;

  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (!free_list_.empty()) {
    frameID = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frameID)) {
      latch_.unlock();
      // in case no slot is available we will return null pointer 
      return nullptr;
    }
	replace_P = &pages_[frameID];
    if (replace_P->is_dirty_) {
      FlushPageImpl(replace_P->page_id_);
      replace_P->is_dirty_ = false;
    }
    page_table_.erase(replace_P->page_id_);
  }

  *page_id = disk_manager_->AllocatePage();
  pages_[frameID].page_id_ = *page_id;
// inserting the page ID in page table
  page_table_.insert({*page_id, frameID});
  pages_[frameID].ResetMemory();
  // it will pin the frame ID
  replacer_->Pin(frameID);
  pages_[frameID].pin_count_ = 1;
  pages_[frameID].is_dirty_ = false; 
  latch_.unlock();
  // the page's pointer is returned
  return &pages_[frameID];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  
	frame_id_t frameID;
  Page *delete_P;
	
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  
	// it will search page on page table using the page_id
	// if it does finds the page it will deallocate it and will return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  frameID = page_table_[page_id];
  delete_P = &pages_[frameID];

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // in case the pin count is not 0, it means the page is in use; so it will return false in this case.
  if (delete_P->GetPinCount() > 0) {
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(delete_P->page_id_);
  delete_P->ResetMemory();
  delete_P->page_id_ = INVALID_PAGE_ID;
  delete_P->is_dirty_ = false;
  delete_P->pin_count_ = 0;
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub