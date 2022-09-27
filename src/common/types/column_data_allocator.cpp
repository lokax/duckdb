#include "duckdb/common/types/column_data_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/column_data_collection_segment.hpp"

namespace duckdb {

ColumnDataAllocator::ColumnDataAllocator(Allocator &allocator) : type(ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
	alloc.allocator = &allocator;
}

ColumnDataAllocator::ColumnDataAllocator(BufferManager &buffer_manager)
    : type(ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
	alloc.buffer_manager = &buffer_manager;
}

ColumnDataAllocator::ColumnDataAllocator(ClientContext &context, ColumnDataAllocatorType allocator_type)
    : type(allocator_type) {
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
		alloc.buffer_manager = &BufferManager::GetBufferManager(context);
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		alloc.allocator = &Allocator::Get(context);
		break;
	default:
		throw InternalException("Unrecognized column data allocator type");
	}
}

BufferHandle ColumnDataAllocator::Pin(uint32_t block_id) {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	return alloc.buffer_manager->Pin(blocks[block_id].handle);
}

void ColumnDataAllocator::AllocateBlock() {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	BlockMetaData data;
	data.size = 0;
	data.capacity = Storage::BLOCK_ALLOC_SIZE;
	data.handle = alloc.buffer_manager->RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	blocks.push_back(move(data));
}

void ColumnDataAllocator::AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset,
                                       ChunkManagementState *chunk_state) {
    // 内存分配器直接分配
	if (type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
		// in-memory allocator
		auto allocated = alloc.allocator->Allocate(size);
		auto pointer_value = uintptr_t(allocated.get());
        // 如果是32位架构
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			block_id = uint32_t(pointer_value);
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
            // 64位架构，block id存低位
			block_id = uint32_t(pointer_value & 0xFFFFFFFF);
			offset = uint32_t(pointer_value >> 32);
		} else {
			throw InternalException("ColumnDataCollection: Architecture not supported!?");
		}
        // 保存所有权
		allocated_data.push_back(move(allocated));
		return;
	}
    // 分配新块
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateBlock();
		if (chunk_state && !blocks.empty()) {
			auto &last_block = blocks.back();
			auto new_block_id = blocks.size() - 1;
			auto pinned_block = alloc.buffer_manager->Pin(last_block.handle);
            // 保存所有权
			chunk_state->handles[new_block_id] = move(pinned_block);
		}
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = blocks.size() - 1;
	offset = block.size;
	block.size += size;
}

// 这个怎么这么诡异？
void ColumnDataAllocator::Initialize(ColumnDataAllocator &other) {
	D_ASSERT(other.HasBlocks());
	blocks.push_back(other.blocks.back());
}


data_ptr_t ColumnDataAllocator::GetDataPointer(ChunkManagementState &state, uint32_t block_id, uint32_t offset) {
    // 构造指针
	if (type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
		// in-memory allocator: construct pointer from block_id and offset
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			uintptr_t pointer_value = uintptr_t(block_id);
			return (data_ptr_t)pointer_value;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			uintptr_t pointer_value = (uintptr_t(offset) << 32) | uintptr_t(block_id);
			return (data_ptr_t)pointer_value;
		} else {
			throw InternalException("ColumnDataCollection: Architecture not supported!?");
		}
	}
    // 直接p拿pin handle的数据
	D_ASSERT(state.handles.find(block_id) != state.handles.end());
	return state.handles[block_id].Ptr() + offset;
}

Allocator &ColumnDataAllocator::GetAllocator() {
	return type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR ? *alloc.allocator
	                                                            : alloc.buffer_manager->GetBufferAllocator();
}

void ColumnDataAllocator::InitializeChunkState(ChunkManagementState &state, ChunkMetaData &chunk) {
	if (type != ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
		// nothing to pin
		return;
	}
	// release any handles that are no longer required
	bool found_handle;
	do {
		found_handle = false;
        // 遍历每一个handle
		for (auto it = state.handles.begin(); it != state.handles.end(); it++) {
            // block_ids中找到，说明还是需要的数据，就continue，不需要Unpin
			if (chunk.block_ids.find(it->first) != chunk.block_ids.end()) {
				// still required: do not release
				continue;
			}
			state.handles.erase(it);
			found_handle = true;
            // 这里break是因为迭代器失效的问题
			break;
		}
	} while (found_handle);

	// grab any handles that are now required
	for (auto &block_id : chunk.block_ids) {
		if (state.handles.find(block_id) != state.handles.end()) {
			// already pinned: don't need to do anything
			continue;
		}
		state.handles[block_id] = Pin(block_id);
	}
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

} // namespace duckdb
