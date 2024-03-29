// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

#include "db/dbformat.h"
#include "util/debug.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        ///////////meggie
        set_range_(false),
        ///////////meggie
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      //////////////meggie
      if(set_range_ && i == range_index_) {
          children_[i].Seek(range_start_);
      }
      else {
      //////////////meggie
          // DEBUG_T("MergingIterator, before seek to first\n");
          // DEBUG_T("this iter:%p\n", children_[i]);
          children_[i].SeekToFirst();
          //DEBUG_T("MergingIterator, after seek to first\n");
      }
    }
    //DEBUG_T("to find smallest\n");
    FindSmallest();
    //DEBUG_T("after find smallest\n");
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    //DEBUG_T("merge iter, after assert valid\n");
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  ////////////////meggie
  void SetChildRange(int index, Slice start, 
          Slice end, bool containsend) {
     range_index_ = index;
     range_start_ = start;
     range_end_ = end;
     range_containsend_ = containsend;
     set_range_ = true;
	 user_comparator_ = BytewiseComparator();
  }
  ////////////////meggie


 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;

  ////////////meggie
  bool set_range_;
  int range_index_;
  Slice range_start_;
  Slice range_end_;
  bool range_containsend_;
  bool Range_iter_valid();
  const Comparator* user_comparator_;
  ////////////meggie

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

////////////meggie
bool MergingIterator::Range_iter_valid() {
    IteratorWrapper* child = &children_[range_index_];
    if(!child->Valid())
        return false;
	InternalKey ikey, end_ikey;
	ikey.DecodeFrom(child->key());
	end_ikey.DecodeFrom(range_end_);
    if((range_containsend_ && user_comparator_->Compare(ikey.user_key(),
                end_ikey.user_key()) > 0) || (!range_containsend_ && 
					user_comparator_->Compare(ikey.user_key(),
						end_ikey.user_key()) >= 0))
        return false;
    return true;
}
////////////meggie

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];

    ////////////////meggie
    if(set_range_) {
       if(i == range_index_ && Range_iter_valid() || 
               (i != range_index_ && child->Valid())){
        if (smallest == nullptr) {
            smallest = child;
        } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
            smallest = child;
        }
       } 
    } else {
      if(child->Valid()) {
        if (smallest == nullptr) {
            smallest = child;
        } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
            smallest = child;
        }
      }
    }
    ////////////////meggie
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    DEBUG_T("NewEmptyIterator\n");
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
