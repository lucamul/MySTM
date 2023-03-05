#include "macros.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <thread>
#include <shared_mutex>
#include <string.h>
#include <tm.hpp>
#include <unordered_set>
#include <vector>

/*
    This is my implementation of the tl2 stm.
*/

#define NUM_SEGMENTS 512
#define NUM_WORDS 2048

// auxiliar structure used in the TimestampLock class
struct Lock {
    bool is_locked;
    uint64_t stamp, lock;
};

// auxiliary function returning whether a Lock is free or not
bool is_locked(Lock lock){
    return lock.is_locked;
}


class TimestampLock {
private:
// serialized value
  std::atomic_uint64_t value;

public:
  TimestampLock() : value(0) {}
  TimestampLock(const TimestampLock &timestamp) { 
    value = timestamp.value.load(); 
  }

  Lock getValue(){
    // get serialized value, first 63 bits are stamp, last bit the lock
    uint64_t val = this->value.load();
    uint64_t stamp, lock;
    stamp = (((uint64_t)1 << 63) - 1) & val;
    lock = val >> 63;
    
    // translate into a Lock data structure and return it
    Lock result;
    if(lock == 1) result.is_locked = true;
    else result.is_locked = false;
    result.stamp = stamp;
    result.lock = val;
    return result;
  }
  
  bool CandS(bool is_locked, uint64_t stamp, uint64_t val){
    if((stamp >> 63) == 1) throw -1; // Technically we may have so many versions that we overflow the 63 bits
    uint64_t new_value = stamp;
    // is it is locked then set the new value as version + locked bit = 1 else  locked bit = 0
    if(is_locked) new_value = ((uint64_t)1 << 63) | stamp;
    // compare and swap
    return this->value.compare_exchange_strong(val,new_value);
  }

  bool getLock(){
    // get value if unlocked by locking it and CandS
    Lock lock = this->getValue();
    if(is_locked(lock)) return false;
    return CandS(true,lock.stamp,lock.lock);
  }

  bool freeLock(bool set_a_new_timestamp, uint64_t new_value){
    // free lock and optionally change timestamp
    Lock lock = this->getValue();
    if(!is_locked(lock)) return false;
    if(set_a_new_timestamp) return this->CandS(false, new_value, lock.lock);
    else return this->CandS(false,lock.stamp,lock.lock);
  }
};

// this is the tx_t identifier for a transaction
struct Tid{
    // if it is ro
    bool is_readonly;
    // stamps for read and writes
    uint64_t read, write;
    // set of read words
    std::unordered_set<void*> read_words;
    // map of target->source written words
    std::map<uintptr_t, void*> target_source_writemap;
};

// instantiate one instance of a TimeStamp lock for every word
struct LockPerWord{
    LockPerWord() : lock(), word_id(0) {}
    TimestampLock lock;
    uint64_t word_id;
};

// shared_t identifier for a region
struct MemoryRegion{
    MemoryRegion(size_t size, size_t align) : size(size), align(align), segments(NUM_SEGMENTS, std::vector<LockPerWord>(NUM_WORDS)) {}
    size_t size, align;
    // number of allocated segments (atomic variable)
    std::atomic_uint64_t number_of_segments = 2;
    // list of list of words, the size is static and the constants determined by trial and error
    std::vector<std::vector<LockPerWord>> segments;
};

// generates new timestamp atomically
static std::atomic_uint stamp_generator = 0;
// each thread as a unique Tid and it gets initialized anew after every txn
static thread_local Tid transaction_id;


// provided with a ptr to the shared memory it recovers the address of the segment and the word by making use of the first 32 bits and last 32 bits
std::pair<uint64_t,uint64_t> getAddresses(shared_t shared, uintptr_t target){
    size_t align = ((MemoryRegion*)shared)->align;
    std::pair<uint64_t,uint64_t> result(target >> 32, ((target << 32)>>32) / align);
    return result;
}

void init_transaction(){
    // clear sets, free words in write set, set timestamps to 0 for reads, set readonly to false
    transaction_id.read_words.clear();
    // free write set words
    for(auto &ptr : transaction_id.target_source_writemap){
        free(ptr.second);
    }
    transaction_id.target_source_writemap.clear();
    transaction_id.read = 0;
    transaction_id.is_readonly = false;
}

shared_t tm_create(size_t size, size_t align) noexcept {
    // initialize memory region
    MemoryRegion* shared = new struct MemoryRegion(size,align);
    if(unlikely(!shared)) return invalid_shared;
    return shared;
}

void tm_destroy(shared_t shared) noexcept {
    // call delete to free memory region
  delete ((struct MemoryRegion*) shared);
}

// the start is a static address
void *tm_start(shared_t unused(shared)) noexcept {
  return (void *)((uint64_t)1 << 32);
}

size_t tm_size(shared_t shared) noexcept {
  return ((struct MemoryRegion*)shared)->size;
}

size_t tm_align(shared_t shared) noexcept {
  return ((struct MemoryRegion*)shared)->align;
}

tx_t tm_begin(shared_t unused(shared), bool is_ro) noexcept {
    // get new value of read stamp, set readonly
    transaction_id.read = stamp_generator.load();
    transaction_id.is_readonly = is_ro;
    return (uintptr_t)&transaction_id;   
}

bool tm_write(shared_t shared, tx_t unused(tx), void const *source, size_t size,
              void *target) noexcept {

    size_t align = tm_align(shared);
    // for every word
    for(size_t i = 0; i < size/align; i++){
        uintptr_t tgt = (uintptr_t)target + align*i;
        void* src = (void*)((uintptr_t)source + align*i);
        // allocate a temporary space for the word
        void* temporary_var = malloc(align);
        // write onto the temporary space
        memcpy(temporary_var,src,align);
        // save the write into the target->source map
        transaction_id.target_source_writemap[tgt] = temporary_var;
    }
    return true;
}

bool tm_read(shared_t shared, tx_t unused(tx), void const *source, size_t size,
             void *target) noexcept {
    size_t align = tm_align(shared);
    
    for(size_t i = 0 ; i < size/align; i++){
        uintptr_t src = (uintptr_t)source + align*i;
        // get the address of the word
        std::pair<uint64_t,uint64_t> adds = getAddresses(shared, src);
        LockPerWord &lock = ((MemoryRegion*)shared)->segments[adds.first][adds.second];
        void* tgt = (void*) ((uintptr_t)target + align*i);
        if(!transaction_id.is_readonly){
            // look for the word in your write set, if found read that
            auto word = transaction_id.target_source_writemap.find(src);
            if(word != transaction_id.target_source_writemap.end()){
                memcpy(tgt, word->second, align);
                continue;
            }
        }
        // get lock of word
        Lock val = lock.lock.getValue();
        // now you can read the value
        memcpy(tgt,&lock.word_id,align);
        // get new lock
        Lock new_val = lock.lock.getValue();
        // if the word was locked at the end, or the stamp changed or the stamp is newer then your read stamp then conflict, ABORT
        if(new_val.is_locked || (val.stamp != new_val.stamp) || (val.stamp > transaction_id.read)){
            init_transaction();
            return false;
        }

        if(!transaction_id.is_readonly){
            // if the transaction is not read only save the read word in your set, you'll need to validate
            transaction_id.read_words.emplace((void*)src);
        }
    }
    return true;
}

bool tm_end(shared_t shared, tx_t unused(tx)) noexcept{
    // if tx is readonly or nothing was written anyway just init new transaction and commit
    if(transaction_id.is_readonly){
        init_transaction();
        return true;
    }
    if(transaction_id.target_source_writemap.empty()){
        init_transaction();
        return true;
    }

    // here we try to get the lock for each word we have ""written"" in our map, if any lock is not available we release all and ABORT
    MemoryRegion* region = (MemoryRegion*)shared;
    int i = 0;
    for(auto & pair : transaction_id.target_source_writemap){
        std::pair<uint64_t,uint64_t> adds = getAddresses(region, pair.first);
        LockPerWord &lock = region->segments[adds.first][adds.second];
        bool got_the_lock = lock.lock.getLock();
        if(got_the_lock == false){
            for(auto &pair : transaction_id.target_source_writemap){
                if(i == 0) break;
                std::pair<uint64_t,uint64_t> adds = getAddresses(region, pair.first);
                LockPerWord &lock = region->segments[adds.first][adds.second];
                lock.lock.freeLock(false,0);
                if(i <= 1) break;
                i--;
            }
            init_transaction();
            return false;
        }
        i++;
    }

    // increase our stamp for writes
    transaction_id.write = stamp_generator.fetch_add(1) + 1;

    // if the read is not 1 step behind the write release all locks and abort
    if(transaction_id.read != transaction_id.write - 1){
        for(auto word: transaction_id.read_words){
            std::pair<uint64_t,uint64_t> adds = getAddresses(region, (uintptr_t)word);
            LockPerWord &lock = region->segments[adds.first][adds.second];
            Lock value = lock.lock.getValue();
            if(value.is_locked || value.stamp > transaction_id.read){
                for(auto &pair : transaction_id.target_source_writemap){
                    if(i==0) break;
                    std::pair<uint64_t,uint64_t> adds = getAddresses(region, pair.first);
                    LockPerWord &lock = region->segments[adds.first][adds.second];
                    lock.lock.freeLock(false,0);
                    if(i <= 1) break;
                    i--;
                }
                init_transaction();
                return false;
            }
        }
    }
    // now we own all locks for writes we can actually save them into shared memory then free the locks and commit
    for(auto pair : transaction_id.target_source_writemap){
        std::pair<uint64_t,uint64_t> adds = getAddresses(region, (uintptr_t)pair.first);
        LockPerWord &lock = region->segments[adds.first][adds.second];
        memcpy(&lock.word_id,pair.second,region->align);
        if(!lock.lock.freeLock(true,transaction_id.write)){
            init_transaction();
            return false;
        }
    }
    init_transaction();
    return true;
}

// add a new segment to number of segments and return that address in the first half of the ptr to point to word 0 of the segment
Alloc tm_alloc(shared_t shared, tx_t unused(tx), size_t unused(size),
               void **target) noexcept {
  *target = (void*)(((MemoryRegion*)shared)->number_of_segments.fetch_add(1) << 32);
  return Alloc::success;
}

// nothing to be done as the segments are statically allocated anyway and the constants are tuned so we never need to make space
bool tm_free(shared_t unused(shared), tx_t unused(tx),
             void *unused(segment)) noexcept {
  return true;
}