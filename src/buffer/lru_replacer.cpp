//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  frame_list_.clear();
  iter_map_.clear();
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> guard(lock_);
  if (frame_list_.empty()) {
    frame_id = NULL;
    return false;
  }
  frame_id_t last_id = frame_list_.back();
  *frame_id = last_id;
  iter_map_.erase(last_id);
  frame_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // remove from replacer
  lock_guard<mutex> guard(lock_);
  if (!iter_map_.count(frame_id)) {
    return;
  }
  auto iter = iter_map_[frame_id];
  frame_list_.erase(iter);
  iter_map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // insert into replacer
  lock_guard<mutex> guard(lock_);
  if (iter_map_.count(frame_id)) {
    //frame_list_.erase(iter_map_[frame_id]);
    return; // why???
  }
  frame_list_.push_front(frame_id);
  iter_map_[frame_id] = frame_list_.begin();
}

size_t LRUReplacer::Size() {
  lock_guard<mutex> guard(lock_);
  return frame_list_.size();
}  // namespace bustub

}
