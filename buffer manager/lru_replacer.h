//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);
  // check if frame_id < num_pages
  // if frame_id > num_pages, there is an issue
  
  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  // remove
  // void printReplacer() override;

  // frame_id_t and page_id_t are simply aliases for 32-bit integer
  bool Victim(frame_id_t *frame_id) override;

  //
  void Pin(frame_id_t frame_id) override;

  // unpin passes frame_id
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!

  // You need a data structure that supports FIFO.. keeps track of frames that are eligible to be replaced
  // will store the least recently unpinned frame
  // I advise you use an std::list -> #include<list>
  // std::list<frame_id_t> - initially empty
  std::list<frame_id_t> LRU;

  // I need a replacer variable - size varaible - num_pages 7 for test - line 33
  size_t number_pages;
};

}  // namespace bustub
