from bisect import bisect_left
import struct

"""
This code sample generated by ChatGPT
"""
class BTreeNode:
    def __init__(self, keys=None, children=None, page_number=None, dirty=False):
        self.keys = keys or []
        self.children = children or []
        self.page_number = page_number
        self.dirty = dirty

    def __repr__(self):
        return f'BTreeNode({self.keys}, {self.children}, {self.page_number}, {self.dirty})'

    def is_leaf(self):
        return not self.children


class BTree:
    def __init__(self, order, page_size, file):
        self.root = BTreeNode()
        self.order = order
        self.page_size = page_size
        self.file = file
        if self.file_size() == 0:
            self.write_page(self.root)

    def search(self, key):
        node, index = self._find_node(key)
        if index < len(node.keys) and node.keys[index] == key:
            return True
        else:
            return False

    def insert(self, key):
        node, index = self._find_node(key)
        if index < len(node.keys) and node.keys[index] == key:
            return False
        else:
            self._insert(node, key)
            return True

    def _insert(self, node, key):
        node.keys.append(key)
        node.keys.sort()
        if len(node.keys) > self.order:
            mid = len(node.keys) // 2
            left_node = BTreeNode(node.keys[:mid], node.children[:mid+1])
            right_node = BTreeNode(node.keys[mid+1:], node.children[mid+1:])
            node.keys = [node.keys[mid]]
            node.children = [left_node, right_node]
            self.write_page(left_node)
            self.write_page(right_node)
        self.write_page(node)

    def _find_node(self, key):
        node = self.read_page(self.root.page_number)
        while not node.is_leaf():
            index = bisect_left(node.keys, key)
            node = self.read_page(node.children[index].page_number)
        index = bisect_left(node.keys, key)
        return node, index

    def write_page(self, node):
        if node.page_number is None:
            node.page_number = self.allocate_page()
        with open(self.file, 'r+b') as f:
            f.seek(node.page_number * self.page_size)
            f.write(self.serialize(node))

    def read_page(self, page_number):
        with open(self.file, 'rb') as f:
            f.seek(page_number * self.page_size)
            data = f.read(self.page_size)
        node = self.deserialize(data)
        node.page_number = page_number
        return node

    def allocate_page(self):
        return self.file_size() // self.page_size

    def file_size(self):
        with open(self.file, 'rb') as f:
            f.seek(0, 2)
            return f.tell()

    def serialize(self, node):
        number_of_keys = len(node.keys)
        data = struct.pack('i', number_of_keys)
        data += struct.pack(f'{len(node.children)}q', *[child.page_number for child in node.children])
        return data

    def deserialize(self, data):
        num_keys = len(data) // 12
        keys = struct.unpack(f'{num_keys}i', data[:num_keys*4])
        if num_keys > 0:
            children = struct.unpack(f'{num_keys+1}q', data[num_keys*4:])
        else:
            children = []
        return BTreeNode(keys, children[:-1])

    def __repr__(self):
        return f'BTree({self.root}, {self.order}, {self.page_size}, {self.file})'

if __name__ == "__main__":
    open("my_btree.db", "w").close()
    btree = BTree(order=3, page_size=4096, file="my_btree.db")
    btree.insert(10)
    print(btree)
    # btree.insert(20)
    # btree.insert(30)
    # btree.insert(40)
    # btree.insert(50)
    # result = btree.search(20)
    # print(result)  # True

