#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {
uint8_t &IteratorCurrentKey::operator[](idx_t idx) {
	if (idx >= key.size()) {
		key.push_back(0);
	}
	D_ASSERT(idx < key.size());
	return key[idx];
}

//! Push Byte
void IteratorCurrentKey::Push(uint8_t byte) {
	if (cur_key_pos == key.size()) {
		key.push_back(byte);
	}
	D_ASSERT(cur_key_pos < key.size());
	key[cur_key_pos++] = byte;
}
//! Pops n elements
void IteratorCurrentKey::Pop(idx_t n) {
	cur_key_pos -= n;
	D_ASSERT(cur_key_pos <= key.size());
}

bool IteratorCurrentKey::operator>(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos > k.len;
}

bool IteratorCurrentKey::operator>=(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos >= k.len;
}

bool IteratorCurrentKey::operator==(const Key &k) const {
	if (cur_key_pos != k.len) {
		return false;
	}
	for (idx_t i = 0; i < cur_key_pos; i++) {
		if (key[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

// 查找最小值
void Iterator::FindMinimum(Node &node) {
	Node *next = nullptr;
	idx_t pos = 0;
    // 把前缀push到cur_key中
	// reconstruct the prefix
	for (idx_t i = 0; i < node.prefix.Size(); i++) {
		cur_key.Push(node.prefix[i]);
	}
	switch (node.type) {
	case NodeType::NLeaf:
        // 设置一下last_leaf
		last_leaf = (Leaf *)&node;
		return;
	case NodeType::N4: {
        // 最左边的孩子
		next = ((Node4 &)node).children[0].Unswizzle(*art);
        // push进去第一个key，至于前缀，已经在上面处理过了
		cur_key.Push(((Node4 &)node).key[0]);
		break;
	}
	case NodeType::N16: {
		next = ((Node16 &)node).children[0].Unswizzle(*art);
		cur_key.Push(((Node16 &)node).key[0]);
		break;
	}
	case NodeType::N48: {
		auto &n48 = (Node48 &)node;
		while (n48.child_index[pos] == Node::EMPTY_MARKER) {
			pos++;
		}
		cur_key.Push(pos);
		next = n48.children[n48.child_index[pos]].Unswizzle(*art);
		break;
	}
	case NodeType::N256: {
		auto &n256 = (Node256 &)node;
		while (!n256.children[pos].pointer) {
			pos++;
		}
		cur_key.Push(pos);
		next = (Node *)n256.children[pos].Unswizzle(*art);
		break;
	}
	}
    // 把这个位置推到栈上
	nodes.push(IteratorEntry(&node, pos));
    // 递归进行处理
	FindMinimum(*next);
}

void Iterator::PushKey(Node *cur_node, uint16_t pos) {
	switch (cur_node->type) {
	case NodeType::N4:
		cur_key.Push(((Node4 *)cur_node)->key[pos]);
		break;
	case NodeType::N16:
		cur_key.Push(((Node16 *)cur_node)->key[pos]);
		break;
	case NodeType::N48:
	case NodeType::N256:
		cur_key.Push(pos);
		break;
	case NodeType::NLeaf:
		break;
	}
}

bool Iterator::Scan(Key *bound, idx_t max_count, vector<row_t> &result_ids, bool is_inclusive) {
	bool has_next;
	do {
		if (bound) {
			if (is_inclusive) {
				if (cur_key > *bound) {
					break;
				}
			} else {
				if (cur_key >= *bound) {
					break;
				}
			}
		}
		if (result_ids.size() + last_leaf->count > max_count) {
			// adding these elements would exceed the max count
			return false;
		}
		for (idx_t i = 0; i < last_leaf->count; i++) {
			row_t row_id = last_leaf->GetRowId(i);
			result_ids.push_back(row_id);
		}
		has_next = Next();
	} while (has_next);
	return true;
}

bool Iterator::Next() {
	if (!nodes.empty()) {
		auto cur_node = nodes.top().node;
		if (cur_node->type == NodeType::NLeaf) {
			// Pop Leaf (We must pop the prefix size + the key to the node (unless we are popping the root)
			idx_t elements_to_pop = cur_node->prefix.Size() + (nodes.size() != 1);
			cur_key.Pop(elements_to_pop);
			nodes.pop();
		}
	}

	// Look for the next leaf
	while (!nodes.empty()) {
		// cur_node
		auto &top = nodes.top();
		Node *node = top.node;
		if (node->type == NodeType::NLeaf) {
			// found a leaf: move to next node
			last_leaf = (Leaf *)node;
			return true;
		}
		// Find next node
		top.pos = node->GetNextPos(top.pos);
		if (top.pos != DConstants::INVALID_INDEX) {
			// add key-byte of the new node
            // push另一个key进去
			PushKey(node, top.pos);
            // 拿出孩子
			auto next_node = node->GetChild(*art, top.pos);
            // 孩子的前缀要加进去
			// add prefix of new node
			for (idx_t i = 0; i < next_node->prefix.Size(); i++) {
				cur_key.Push(next_node->prefix[i]);
			}
			// next node found: push it
            // 这个地方为什么是INVALID_INDEX
			nodes.push(IteratorEntry(next_node, DConstants::INVALID_INDEX));
		} else {
			// no node found: move up the tree and Pop prefix and key of current node
			auto cur_node = nodes.top().node;
			idx_t elements_to_pop = cur_node->prefix.Size() + (nodes.size() != 1);
			cur_key.Pop(elements_to_pop);
			nodes.pop();
		}
	}
	return false;
}

bool Iterator::LowerBound(Node *node, Key &key, bool inclusive) {
	bool equal = false;
	if (!node) {
		return false;
	}
	idx_t depth = 0;
	while (true) {
		nodes.push(IteratorEntry(node, 0));
		auto &top = nodes.top();
		// reconstruct the prefix
        // 推当前node的前缀进去
		for (idx_t i = 0; i < top.node->prefix.Size(); i++) {
			cur_key.Push(top.node->prefix[i]);
		}
		if (!equal) {
            // 一直向左遍历到叶子节点?
			while (node->type != NodeType::NLeaf) {
                // 拿出node最小的位置
				auto min_pos = node->GetMin();
                // push最小位置的key到cur_key中
				PushKey(node, min_pos);
				nodes.push(IteratorEntry(node, min_pos));
				node = node->GetChild(*art, min_pos);
				// reconstruct the prefix
				for (idx_t i = 0; i < node->prefix.Size(); i++) {
					cur_key.Push(node->prefix[i]);
				}
				auto &c_top = nodes.top();
				c_top.node = node;
			}
		}
		if (node->type == NodeType::NLeaf) {
			// found a leaf node: check if it is bigger or equal than the current key
			auto leaf = static_cast<Leaf *>(node);
			last_leaf = leaf;
			// if the search is not inclusive the leaf node could still be equal to the current value
			// check if leaf is equal to the current key
			if (cur_key == key) {
				// if it's not inclusive check if there is a next leaf
				if (!inclusive && !Next()) {
					return false;
				} else {
					return true;
				}
			}

			if (cur_key > key) {
				return true;
			}
			// Leaf is lower than key
			// Check if next leaf is still lower than key
			while (Next()) {
				if (cur_key == key) {
					// if it's not inclusive check if there is a next leaf
					if (!inclusive && !Next()) {
						return false;
					} else {
						return true;
					}
				} else if (cur_key > key) {
					// if it's not inclusive check if there is a next leaf
					return true;
				}
			}
			return false;
		}
        // 当前是内部节点
        // 以下代码很奇怪
        // 这里好像流程不会走过来？是bug？
		uint32_t mismatch_pos = node->prefix.KeyMismatchPosition(key, depth);
        // 前缀不匹配
		if (mismatch_pos != node->prefix.Size()) {
            // 并且前缀小于我们的key
			if (node->prefix[mismatch_pos] < key[depth + mismatch_pos]) {
				// Less
                // 直接弹nodes？不弹cur_key?
				nodes.pop();
				return Next();
			} else {
				// Greater
				top.pos = DConstants::INVALID_INDEX;
				return Next();
			}
		}
		// prefix matches, search inside the child for the key
		depth += node->prefix.Size();
        // 找到下一个大于等于当前key的节点
		top.pos = node->GetChildGreaterEqual(key[depth], equal);
        // 为什么要找最小叶子?
		if (top.pos == DConstants::INVALID_INDEX) {
			// Find min leaf
			top.pos = node->GetMin();
		}
		node = node->GetChild(*art, top.pos);
		// This means all children of this node qualify as geq
		depth++;
	}
}

} // namespace duckdb
