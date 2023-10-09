
#include <cassert>
#include <iostream>
#include <string>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"


namespace buzzdb {

char* BufferFrame::get_data() {
    auto returnData = reinterpret_cast<char *>(this->data.data());
    return returnData;
}


BufferManager::BufferManager(size_t page_size, size_t page_count) {
    this->pageCount = page_count;
    this->pageSize = page_size;
}


BufferManager::~BufferManager() {
    for (size_t i = 0; i < fifoVector.size(); i++) {
        BufferFrame& frame = *fifoVector[i];
        if (frame.dirtyBit == true){
            uint16_t segment_id = BufferManager::get_segment_id(frame.page_id);
            uint64_t offset = BufferManager::get_segment_page_id(frame.page_id) * pageSize;
            char* block = frame.get_data();
            auto file_handle = File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
            file_handle->write_block(block, offset, pageSize);
        }
        //write to disk
    }
    for (size_t i = 0; i < lruVector.size(); i++) {
        BufferFrame& lru_frame = *lruVector[i];
        if (lru_frame.dirtyBit == true){
            uint16_t lru_segment_id = BufferManager::get_segment_id(lru_frame.page_id);
            uint64_t lru_offset = BufferManager::get_segment_page_id(lru_frame.page_id) * pageSize;
            char* lru_block = lru_frame.get_data();
            auto lru_file_handle = File::open_file(std::to_string(lru_segment_id).c_str(), File::WRITE);
            lru_file_handle->write_block(lru_block, lru_offset, pageSize);
        }
    }
}


BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
    BufferFrame new_page;
    //checking lru, if inside, move to back
    for (size_t i = 0; i < lruVector.size(); i++) {
        BufferFrame& lru_frame = *lruVector[i];
        if (lru_frame.page_id == page_id){
            new_page.page_id = page_id;
            new_page.data = std::move(lru_frame.data);
            new_page.dirtyBit = lru_frame.dirtyBit;
            new_page.fixed = true;
            
            lruVector.erase(lruVector.begin() + i);
            lruVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
            return *lruVector.back();
        }
    }
    //checking fifo, if inside, upgrade
    //might need to check if enough memory
    for (size_t j = 0; j < fifoVector.size(); j++) {
        BufferFrame& frame = *fifoVector[j];
        if (frame.page_id == page_id){
            new_page.page_id = page_id;
            new_page.data = std::move(frame.data);
            new_page.dirtyBit = frame.dirtyBit;
            new_page.fixed = true;
            fifoVector.erase(fifoVector.begin()+j);
            lruVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
            
            return *lruVector.back();
        }
    }
    //check if free space possible, if there is, put in fifo,
    //else {

    std::vector<uint64_t> diskData;
    diskData.resize(pageSize / sizeof(uint64_t));
    uint16_t segment_id = BufferManager::get_segment_id(page_id);
    uint64_t offset = BufferManager::get_segment_page_id(page_id) * pageSize;
    auto file_handle = File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
    char* block = reinterpret_cast<char *>(diskData.data());
    file_handle->read_block(offset, pageSize, block);
    new_page.page_id = page_id;
    new_page.data = std::move(diskData);
    new_page.fixed = true;
    new_page.exclusive = exclusive;



    if ((fifoVector.size() + lruVector.size()) < pageCount) {
        fifoVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
        return *fifoVector.back();
    } else{
            //checking fifo, not dirty and unfixed
        for (size_t k = 0; k < fifoVector.size(); k++) {
            BufferFrame& clean_frame = *fifoVector[k];
            if (clean_frame.dirtyBit == false && clean_frame.fixed == false){
                fifoVector.erase(fifoVector.begin() + k);
                fifoVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
                return *fifoVector.back();
            }
        }
        //checking fifo, dirty and unfixed, write back and replace in fifo
        for (size_t l = 0; l < fifoVector.size(); l++) {
            BufferFrame& dirty_frame = *fifoVector[l];
            if (dirty_frame.dirtyBit == true && dirty_frame.fixed == false){
                uint16_t dirty_segment_id = BufferManager::get_segment_id(dirty_frame.page_id);
                uint64_t dirty_offset = BufferManager::get_segment_page_id(dirty_frame.page_id) * pageSize;
                char* dirty_block = dirty_frame.get_data();
                auto dirty_file_handle = File::open_file(std::to_string(dirty_segment_id).c_str(), File::WRITE);
                dirty_file_handle->write_block(dirty_block, dirty_offset, pageSize);

                fifoVector.erase(fifoVector.begin() + l);
                fifoVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
                return *fifoVector.back();
            }
        }
        //checking lru, not dirty and unfixed, replace. by this point we know all the fifo fixed
        for (size_t m = 0; m < lruVector.size(); m++) {
            BufferFrame& clean_frame = *lruVector[m];
            if (clean_frame.dirtyBit == false && clean_frame.fixed == false){
                lruVector.erase(lruVector.begin() + m);
                lruVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
                return *lruVector.back();
            }
        }
        for (size_t n = 0; n < lruVector.size(); n++) {
            BufferFrame& dirty_frame = *lruVector[n];
            if (dirty_frame.dirtyBit == true && dirty_frame.fixed == false){
                uint16_t dirty_segment_id = BufferManager::get_segment_id(dirty_frame.page_id);
                uint64_t dirty_offset = BufferManager::get_segment_page_id(dirty_frame.page_id) * pageSize;
                char* dirty_block = dirty_frame.get_data();
                auto dirty_file_handle = File::open_file(std::to_string(dirty_segment_id).c_str(), File::WRITE);
                dirty_file_handle->write_block(dirty_block, dirty_offset, pageSize);

                lruVector.erase(lruVector.begin() + n);
                lruVector.push_back(std::move(std::make_unique<BufferFrame>(new_page)));
                return *lruVector.back();
            }
        }
    }
    throw buffer_full_error{};
}



void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
    for (size_t i = 0; i < fifoVector.size(); i++) {
        BufferFrame& frame = *fifoVector[i];
        if (frame.page_id == page.page_id){
            frame.fixed = false;
            frame.dirtyBit = is_dirty;
            return;
        }
    }
    for (size_t i = 0; i < lruVector.size(); i++) {
        BufferFrame& lru_frame = *lruVector[i];
        if (lru_frame.page_id == page.page_id){
            lru_frame.fixed = false;
            lru_frame.dirtyBit = is_dirty;
            return;
        }
    }
    return;
}


std::vector<uint64_t> BufferManager::get_fifo_list() const {
    std::vector<uint64_t> fifoPageIds;
    for (size_t i = 0; i < this->fifoVector.size(); i++) {
        BufferFrame& f = *fifoVector[i];
        fifoPageIds.push_back(f.page_id);
    }
    return fifoPageIds;

}


std::vector<uint64_t> BufferManager::get_lru_list() const {
    std::vector<uint64_t> lruPageIds;
    for (size_t i = 0; i < this->lruVector.size(); i++) {
        BufferFrame& f = *lruVector[i];
        lruPageIds.push_back(f.page_id);
    }
    return lruPageIds;
}

}  // namespace buzzdb
