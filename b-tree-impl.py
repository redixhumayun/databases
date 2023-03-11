from bisect import bisect_left
from enum import Enum
import math

"""
If the order of the tree is (m=3), then:
A node can have a maximum of m children. (i.e. 3)
A node can contain a maximum of m - 1 keys. (i.e. 2)
A node should have a minimum of ceil(m/2) children. (i.e. 2)
A node (except root node) should contain a minimum of ⌈m/2⌉ - 1 keys. (i.e. 1)
"""

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
    def __init__(self, key, left_child_pointer):
        self.key = key
        self.left_child_pointer = left_child_pointer

    def print(self, indent):
        print ('\t' *indent + f"({self.key}, Range: {self.left_child_pointer.cells[0].key}, {self.left_child_pointer.cells[-1].key})")

class BTreeNode:
    def __init__(self, type: NodeType):
        self.type = type or NodeType.LEAF
        self.num_of_cells = 0
        self.cells = []
        self.parent = None
        self.right_child_pointer = None #   This is only used for internal nodes
        self.right_sibling_pointer = None #   This is only used for leaf nodes

    def print(self, indent):
        if self.type is NodeType.LEAF:
            print('\t' * indent + "Leaf Node: ")
            for cell in self.cells:
                cell.print(indent+1)
            if self.right_sibling_pointer:
                print('\t' * (indent + 1) + "Sibling Pointer: ")
                self.right_sibling_pointer.print(indent+2)
        elif self.type is NodeType.INTERNAL:
            print('\t' * indent + "Internal Node: ")
            for cell in self.cells:
                cell.print(indent+1)
            if self.right_sibling_pointer:
                print('\t' * (indent + 1) + "Sibling Pointer: ")
                self.right_sibling_pointer.print(indent+2)

class BTree:
    def __init__(self, order):
        self.root = BTreeNode(NodeType.LEAF)
        self.order = order

    def print(self, root, indent=0):
        if root is None:
            return
        print('\t' * indent + 'Root')
        root.print(indent+1)
        for cell in root.cells:
            if isinstance(cell, InternalNodeCell):
                print('\t' * (indent + 1) + "Left Child:")
                cell.left_child_pointer.print(indent+2)
                if cell.left_child_pointer.type is NodeType.INTERNAL:
                    self.print(cell.left_child_pointer, indent+2)
        if root.type is NodeType.INTERNAL and root.right_child_pointer is not None:
            print ('\t' * (indent + 1) + "Right Child:")
            root.right_child_pointer.print(indent+2)
            if root.right_child_pointer.type is NodeType.INTERNAL:
                self.print(root.right_child_pointer, indent+2)

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
            parent_node = self._insert(node.parent, key_to_promote, node)
            node.parent = parent_node
            sibling_node.parent = parent_node


            parent_insertion_index = bisect_left(node.parent.cells, key_to_promote, key=lambda x: x.key) + 1
            if parent_insertion_index < len(node.parent.cells):
                node.parent.cells[parent_insertion_index].left_child_pointer = sibling_node
            else:
                node.parent.right_child_pointer = sibling_node

            #   Update the sibling pointers
            temp = node.right_sibling_pointer
            node.right_sibling_pointer = sibling_node
            sibling_node.right_sibling_pointer = temp

        if node.type is NodeType.INTERNAL:
            #   Place the key into the correct node
            if insertion_index <= middle_index:
                node.cells.insert(insertion_index, InternalNodeCell(key, value))
                node.num_of_cells += 1
            else:
                sibling_node.cells.insert(insertion_index - middle_index, InternalNodeCell(key, value))
                sibling_node.num_of_cells += 1

            key_to_promote = sibling_node.cells[0].key
            parent_node = self._insert(node.parent, key_to_promote, node)
            node.parent = parent_node
            sibling_node.parent = parent_node

            parent_insertion_index = bisect_left(node.parent.cells, key_to_promote, key=lambda x: x.key) + 1
            if parent_insertion_index < len(node.parent.cells):
                node.parent.cells[parent_insertion_index].left_child_pointer = sibling_node
            else:
                node.parent.right_child_pointer = sibling_node
            
            if insertion_index <= middle_index:
                return node
            return sibling_node

    def delete(self, key):
        node, index = self._search(key)

        #   If the key does not exist in the database
        if index >= len(node.cells) or node.cells[index].key != key:
            return False
        
        #   If the key exists in the database
        self._delete(node, index)

    def _delete(self, node, index):
        if node.type is NodeType.LEAF:
            #   If the node has more than the minimum number of cells
            #   Just delete the required cell and reduce number of cells by 1
            if node.num_of_cells > math.ceil(self.order / 2) - 1:
                node.cells.pop(index)
                node.num_of_cells -= 1
                return True
            else:
                #   If the node has the minimum number of cells
                #   Check if the right sibling shares a parent with the current node
                if node.right_sibling_pointer is not None and node.right_sibling_pointer.parent is node.parent:
                    #   Delete the cell and reduce number by 1
                    node.cells.pop(index)
                    node.num_of_cells -= 1

                    #   Borrow all the cells from the right sibling
                    node.cells.extend(node.right_sibling_pointer.cells)
                    node.num_of_cells += node.right_sibling_pointer.num_of_cells

                    #   Delete the right sibling and remove the separator key from the parent node
                    parent_index_to_remove = bisect_left(node.parent.cells, node.right_sibling_pointer.cells[0].key, key=lambda x: x.key)
                    node.right_sibling_pointer = node.right_sibling_pointer.right_sibling_pointer
                    self._delete(node.parent, parent_index_to_remove)
                    return True
                else:
                    #   If the current node and right sibling do not share a parent and
                    #   removing a cell from the current node will result in less than minimum number of cells
                    #   then just delete the cell. If removing the cell will result in an empty node, remove the node
                    #   and update the parent node
                    temp = node.cells[index].key
                    del node.cells[index]
                    node.num_of_cells -= 1
                    if node.num_of_cells == 0:
                        parent_index = bisect_left(node.parent.cells, temp, key=lambda x: x.key)
                        self._delete(node.parent, parent_index)
                    return True
        elif node.type is NodeType.INTERNAL:
            #   If the node has more than the minimum number of cells
            if node.num_of_cells > math.ceil(self.order / 2) - 1:
                del node.cells[index]
                node.num_of_cells -= 1
                return True
            else:
                #   If node has the minimum number of cells
                #   Check if the right sibling shares a parent with the current node
                if node.right_sibling_pointer is not None and node.right_sibling_pointer.parent is node.parent:
                    #   Delete the cell and reduce number by 1
                    node.cells.pop(index)
                    node.num_of_cells -= 1

                    #   Borrow all the cells from the right sibling
                    node.cells.extend(node.right_sibling_pointer.cells)
                    node.num_of_cells += node.right_sibling_pointer.num_of_cells

                    #   Delete the right sibling and demote the separator key from the parent node into
                    #   the current node
                    node.right_sibling_pointer = node.right_sibling_pointer.right_sibling_pointer
                    parent_index_to_demote = bisect_left(node.parent.cells, node.right_sibling_pointer.cells[0].key, key=lambda x: x.key)
                    parent_cell_to_demote = node.parent.cells[parent_index_to_demote]
                    node_insertion_index = bisect_left(node.cells, node.parent.cells[parent_cell_to_demote].key, key=lambda x: x.key)
                    node.cells.insert(node_insertion_index, node.parent.cells[parent_cell_to_demote])
                    node.num_of_cells += 1
                    self._delete(node.parent, parent_index_to_demote)
                else:
                    #   If the current node and right sibling do not share a parent and
                    #   removing a cell from the current node will result in less than minimum number of cells
                    #   then just delete the cell. If removing the cell will result in an empty node, remove the node
                    #   and update the parent node
                    temp = node.cells[index].key
                    node.cells.pop(index)
                    node.num_of_cells -= 1
                    if node.num_of_cells == 0:
                        parent_index = bisect_left(node.parent.cells, temp, key=lambda x: x.key)
                        self._delete(node.parent, parent_index)
                    return True
    
    def _search(self, key):
        node = self.root
        while node.type is not NodeType.LEAF:
            index = bisect_left(node.cells, key, key=lambda x: x.key)
            if index > len(node.cells) - 1:
                node = node.right_child_pointer
            else:
                node = node.cells[index].left_child_pointer
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
    print(btree.delete(5))
    btree.print(btree.root)