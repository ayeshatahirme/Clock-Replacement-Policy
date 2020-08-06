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

#include "common/logger.h"

namespace bustub {

	int totalPAges;
int clockHand = 1;
ClockReplacer::Frame *head = NULL;
ClockReplacer::Frame *tail = NULL;

void initlizer() {
  for (int i = 0; i < totalPAges; i++) {
    ClockReplacer::Frame *f = new ClockReplacer::Frame;
    f->status = -1;
    f->link = NULL;

    if (head == NULL) {
      head = f;
      tail = f;
    } else {
      tail->link = f;
      tail = f;
    }
  }
  printf("Initilization Done\n");
}

ClockReplacer::Frame *getHead() { return head; }
ClockReplacer::Frame *getTail() { return tail; }

ClockReplacer::Frame *getFrameIndex(int value) {
  ClockReplacer::Frame *temp = head;

  int c = 1;
  while (c < value) {
    temp = temp->link;
    c++;
  }
  return temp;
}

ClockReplacer::ClockReplacer(size_t num_pages) {
  totalPAges = num_pages;
  initlizer();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  ClockReplacer::Frame *frame = head;
  int round = 0;
  while (true) {
    ClockReplacer::Frame *f = getFrameIndex(clockHand);
    if (f->status == 0) {
      *frame_id = clockHand;
      f->status = -1;
      clockHand++;
      if (clockHand == totalPAges) clockHand = 1;
      return true;
    } else if (f->status == 1) {
      f->status = 0;
    }

    round++;
    if (round == 20) {
      return false;
    }

    clockHand++;
    if (clockHand == totalPAges) clockHand = 1;
    frame = frame->link;
  }

  return false;
}

//-1
void ClockReplacer::Pin(frame_id_t frame_id) {
  ClockReplacer::Frame *f = getFrameIndex(frame_id);
  f->status = -1;
}
// 0 first time unpin make it 1
void ClockReplacer::Unpin(frame_id_t frame_id) {
  ClockReplacer::Frame *f = getFrameIndex(frame_id);
  if (f->status == 1) {
    f->status = 0;
  } else if (f->status == -1) {
    f->status = 1;
  }
}

size_t ClockReplacer::Size() {
  ClockReplacer::Frame *temp = head;
  int c = 0;
  while (temp != NULL) {
    if (temp->status == 0 || temp->status == 1) {
      c++;
    }
    temp = temp->link;
  }

  return c;
}
/*
ClockReplacer::ClockReplacer(size_t num_pages) {
  this->ref_flag_.clear();
  this->pin_pos_.clear();
  this->frames_.clear();
  this->clock_hand_ = this->frames_.begin();
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(this->mux_);
  if (this->frames_.size() <= 0) {
    frame_id = nullptr;
    return false;
  }
  while (true) {
    if (this->clock_hand_ == this->frames_.end()) {
      this->clock_hand_ = this->frames_.begin();
    }
    auto f_id = *(this->clock_hand_);
    if (f_id >= 0 && !this->ref_flag_[f_id]) {
      *frame_id = f_id;
      // remove the frame
      this->PinImpl(f_id);
      return true;
    }
    // set ref_flag of f_id to false (cause it's true)
    if (f_id >= 0) {
      this->ref_flag_[f_id] = false;
    }
    // rotate the clock hand
    this->clock_hand_++;
  }
}

// this function is  not thread-safe. Don't call this directly
void ClockReplacer::PinImpl(frame_id_t frame_id) {
  auto found_iterator = this->pin_pos_.find(frame_id);
  if (found_iterator != this->pin_pos_.end()) {
    if (found_iterator->second == this->clock_hand_) {
      this->clock_hand_++;
    }
    this->ref_flag_.erase(frame_id);
    this->frames_.erase(found_iterator->second);
    this->pin_pos_.erase(frame_id);
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(this->mux_);
  this->PinImpl(frame_id);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(this->mux_);
  if (this->ref_flag_.find(frame_id) != this->ref_flag_.end()) {
    this->ref_flag_[frame_id] = true;
    return;
  }
  this->frames_.push_back(frame_id);
  this->ref_flag_.insert({frame_id, true});
  this->pin_pos_.insert({frame_id, std::prev(this->frames_.end())});
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> lock(this->mux_);
  return this->frames_.size();
}*/

}  // namespace bustub




