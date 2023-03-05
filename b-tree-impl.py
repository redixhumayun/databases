from bisect import bisect_left
from enum import Enum
import json

class NodeType(Enum):
    LEAF = 0
    INTERNAL = 1

class LeafNodeCell:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"({self.key}, {self.value})"

    def print(self, indent):
        print ('\t'*indent + f"({self.key}, {self.value})")

class InternalNodeCell:
    def __init__(self, key, right_child_pointer):
        self.key = key
        self.right_child_pointer = right_child_pointer

    def print(self, indent):
        print ('\t' *indent + f"({self.key}, Range: {self.right_child_pointer.cells[0].key}, {self.right_child_pointer.cells[-1].key})")

class BTreeNode:
    def __init__(self, type: NodeType):
        self.type = type or NodeType.LEAF
        self.num_of_cells = 0
        self.cells = []
        self.parent = None
        self.left_child_pointer = None #   This is only used for internal nodes
        self.right_sibling_pointer = None #   This is only used for leaf nodes

    def print(self, indent):
        if self.type is NodeType.LEAF:
            print('\t' * indent + "Leaf Node: ")
            for cell in self.cells:
                cell.print(indent+1)
        elif self.type is NodeType.INTERNAL:
            print('\t' * indent + "Internal Node: ")
            for cell in self.cells:
                cell.print(indent+1)

class BTree:
    def __init__(self, order):
        self.root = BTreeNode(NodeType.LEAF)
        self.order = order

    def print(self, root, indent=0):
        if root is None:
            return
        print('\t' * indent + 'Root')
        root.print(indent+1)
        if root.type is NodeType.INTERNAL and root.left_child_pointer is not None:
            print ('\t' * (indent + 1) + "Left Child:")
            root.left_child_pointer.print(indent+2)
            if root.left_child_pointer.type is NodeType.INTERNAL:
                self.print(root.left_child_pointer, indent+2)
        for cell in root.cells:
            if isinstance(cell, InternalNodeCell):
                print('\t' * (indent + 1) + "Right Child:")
                cell.right_child_pointer.print(indent+2)
                if cell.right_child_pointer.type is NodeType.INTERNAL:
                    self.print(cell.right_child_pointer, indent+2)

    def search(self, key):
        node, index = self._search(key)
        if index < len(node.cells) and node.cells[index].key == key:
            return True
        return False
    
    def get(self, key):
        node, index = self._search(key)
        if index < len(node.cells) and node.cells[index].key == key:
            return node.cells[index].value
        return None
    
    def insert(self, key, value):
        node, index = self._search(key)

        #   If the key already exists in the database
        if index < len(node.cells) and node.cells[index].key == key:
            node.cells[index].value = value
            return True
        
        #   If the key does not currently exist in the database
        self._insert(node, key, value)

    def _insert(self, node, key, value):
        #   Check if the node is full
        insertion_index = bisect_left(node.cells, key, key=lambda x: x.key)
        if node.type is NodeType.LEAF and node.num_of_cells < self.order - 1:
            #   This node will not overflow, add the key value pair
            node.cells.insert(insertion_index, LeafNodeCell(key, value))
            node.num_of_cells += 1
            return node

        if node.type is NodeType.INTERNAL and node.num_of_cells < self.order - 1:
            node.cells.insert(insertion_index, InternalNodeCell(key, value))
            node.num_of_cells += 1
            return node
        
        #   This node will overflow
        
        #   If this node is the root node
        if node.parent is None:
            #   Create a new root node
            new_root = BTreeNode(NodeType.INTERNAL)
            node.parent = new_root
            self.root = new_root

        #   Create a new sibling node first and move half the elements from the current node to the sibling node
        temp_node_cells = node.cells[:]
        sibling_node = BTreeNode(node.type)
        middle_index = node.num_of_cells // 2
        node.cells = node.cells[:middle_index]
        sibling_node.cells = temp_node_cells[middle_index:]
        node.num_of_cells = len(node.cells)
        sibling_node.num_of_cells = len(sibling_node.cells)

        if node.type is NodeType.LEAF:
            #   Place the key into the correct node
            if insertion_index <= middle_index:
                node.cells.insert(insertion_index, LeafNodeCell(key, value))
                node.num_of_cells += 1
            else:
                sibling_node.cells.insert(insertion_index - middle_index, LeafNodeCell(key, value))
                sibling_node.num_of_cells += 1

            key_to_promote = sibling_node.cells[0].key
            parent_node = self._insert(node.parent, key_to_promote, sibling_node)
            node.parent = parent_node
            sibling_node.parent = parent_node
            parent_node.left_child_pointer = node

        if node.type is NodeType.INTERNAL:
            #   Place the key into the correct node
            if insertion_index <= middle_index:
                node.cells.insert(insertion_index, InternalNodeCell(key, value))
                node.num_of_cells += 1
            else:
                sibling_node.cells.insert(insertion_index - middle_index, InternalNodeCell(key, value))
                sibling_node.num_of_cells += 1

            key_to_promote = sibling_node.cells[0].key
            parent_node = self._insert(node.parent, key_to_promote, sibling_node)
            node.parent = parent_node
            sibling_node.parent = parent_node
            parent_node.left_child_pointer = node
            
            if insertion_index <= middle_index:
                return node
            return sibling_node

    def _search(self, key):
        node = self.root
        while node.type is not NodeType.LEAF:
            index = bisect_left(node.cells, key, key=lambda x: x.key)
            if index == 0:
                node = node.left_child_pointer
            else:
                node = node.cells[index - 1].right_child_pointer
        index = bisect_left(node.cells, key, key=lambda x: x.key)
        return node, index

if __name__ == "__main__":
    btree = BTree(3)
    btree.insert(3, 3)
    btree.insert(5, 5)
    btree.insert(1, 1)
    btree.insert(2, 2)
    btree.insert(4, 4)
    btree.insert(6, 6)
    btree.insert(8, 8)
    btree.print(btree.root)