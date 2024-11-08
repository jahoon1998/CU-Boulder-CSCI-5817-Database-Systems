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

// LRU replace is an object that keeps track of which frames in the BP are eligible to be replaced
// replacer = abstract class that keeps track of frames/slots in our BP that are avaiable to be replaced
// with some new page from disk
#include "buffer/lru_replacer.h"
#include <algorithm>
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  // clear the list
  LRU.clear();
  // assign num_pages
  number_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

// Victim = get a frame that should be replaced
// Victim stores frame_id inside of T; i,e, it takes a frame_id as a parameter
// returns whether or not the call was succesfful
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // page_id matches frame_id eventaully
  // First, check if list empty
  if (LRU.empty()) {
    // frame_id = nullptr; There is nothing in the list
    // return false
    frame_id = nullptr;
    return false;
  }
  // if list is not empty
  // go to my list and get the LRU
  // *frame_id = first position in the list
  *frame_id = LRU.front();
  // and remove the frame_id from the list
  LRU.pop_front();
  return true;
}

// Corresponding to pinning a page in the BPM
// Removes frame from the LRU
// Do nothing if the frame isn't in the LRU
// In other words, pin() tells the replacer - this frame is in use and can't  be replaced
void LRUReplacer::Pin(frame_id_t frame_id) {
  // look in your list
  // if it's not there, return - do nothing
  // if it's there, remove it from the list
  bool list_contains_frame_id = std::find(LRU.begin(), LRU.end(), frame_id) != LRU.end();

  if (list_contains_frame_id) {
    LRU.remove(frame_id);
  } else {
    return;
  }
}

// Adds the specified frame into the LRU - std::list<frame_id_t>
// Called by the BPM when the pin count reaches 0
// Does nothing when the frame (frame_id) is already unpinned
// In other words, this frame isn't being used, pin_count = 0; it's eligible to be replaced
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // initially, your list is empty
  // the test case passed in frame id 1, which isn't in the list
  // check if the frame_id_t is in the list
  if (LRU.size() < number_pages) {
    bool list_contains_frame_id = std::find(LRU.begin(), LRU.end(), frame_id) != LRU.end();

    // if the frame found in std::list, it's already been unpinned - do nothing
    if (list_contains_frame_id) {
      // do nothing
      return;
    }
    // if the frame not found in std::list, add to your std::list
    LRU.push_back(frame_id);
    // list will have 1 2 3 4 5 6 if I call unpin 6 times with the frame_ids
  }
}

size_t LRUReplacer::Size() {
  // return the size of the list
  return LRU.size();
}

// void LRUReplacer::printReplacer(){
//   LOG_INFO("Jahoon print Replacer");
//   for (auto v : LRU){
//     LOG_INFO("Jahoon LRU: %d", v);
//   }

// }

}  // namespace bustub
