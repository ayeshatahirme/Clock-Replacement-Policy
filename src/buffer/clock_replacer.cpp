//===----------------------------------------------------------------------===//
//
//                         BusTub
//
//*********************** 2018-CS-22 ************************************
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <cstdio>
#include <thread>
#include <vector>
namespace bustub 
{
ClockReplacer::ClockReplacer(size_t num_pages) 
{
  using std::vector;
  size_t i = 0;
  for (i = 0; i < num_pages; i++) 
{
    in_Frame_Replacer.push_back(-1);
    rf.push_back(true);
  }
  buffer_Frames = num_pages;
  replacer_Frames = clock_Hand = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  process_Lock.lock();
  if (replacer_Frames <= 0) 
  {
    process_Lock.unlock();
    return false;
  }

  while (true) 
  {
    if (rf[clock_Hand] == true) 
	{
      clock_Hand = (clock_Hand + 1) % buffer_Frames;
      continue;
    } 
	else if (in_Frame_Replacer[clock_Hand] == 1) 
	{
      in_Frame_Replacer[clock_Hand] = 0;
      clock_Hand = (clock_Hand + 1) % buffer_Frames;
      continue;
    }
    *frame_id = clock_Hand;
    in_Frame_Replacer[clock_Hand] = -1;
    rf[clock_Hand] = true;
    replacer_Frames = replacer_Frames - 1;
    break;
  }

  process_Lock.unlock();
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) 
{
  process_Lock.lock();
  if (rf[frame_id] != true) 
  {
    in_Frame_Replacer[frame_id] = -1;
    rf[frame_id] = true;
    replacer_Frames = replacer_Frames - 1;
  }
  process_Lock.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) 
{
  process_Lock.lock();
  if (rf[frame_id] == true) 
  {
    in_Frame_Replacer[frame_id] = 1;
    rf[frame_id] = false;
    replacer_Frames = replacer_Frames + 1;
  }
  process_Lock.unlock();
}

size_t ClockReplacer::Size() 
{ 
	return replacer_Frames; 
}
}  // namespace bustub
