//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  frame_list_.clear();
  ref_map_.clear();
  iter_map_.clear();
  clock_hand_ = frame_list_.begin();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> guard(lock_);
  if (frame_list_.empty()) {
    frame_id = NULL;
    return false;
  }
  while (true) {
    // circular buffer
    if (clock_hand_ == frame_list_.end()) {
      clock_hand_ = frame_list_.begin();
    }
    frame_id_t f_id = *clock_hand_;
    if (!ref_map_[f_id]) {
      *frame_id = f_id;
      PinThread(f_id);
      return true;
    }
    ref_map_[f_id] = false;
    ++clock_hand_;
  }
}

void ClockReplacer::PinThread(frame_id_t frame_id) {
  if (!iter_map_.count(frame_id)) {
    return;
  }
  auto iter = iter_map_[frame_id];
  if (clock_hand_ == iter) {
    ++clock_hand_;
  }
  frame_list_.erase(iter);
  ref_map_.erase(frame_id);
  iter_map_.erase(frame_id);
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> guard(lock_);
  PinThread(frame_id);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> guard(lock_);
  if (iter_map_.count(frame_id)) {
    ref_map_[frame_id] = true;
    return;
  }
  frame_list_.push_back(frame_id);
  ref_map_[frame_id] = true;
  iter_map_[frame_id] = --frame_list_.end();
}

size_t ClockReplacer::Size() {
  lock_guard<mutex> guard(lock_);
  return frame_list_.size();
}

}  // namespace bustub
