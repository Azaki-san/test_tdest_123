M = 5

def find_successor(chord, node_id, target):
    for i in range(len(chord)):
        if chord[i] >= target:
            return chord[i]
    return chord[0]


def get_finger_table(chord, node_id):
    finger_table = []
    for i in range(M):
        finger_id = (node_id + 2 ** i) % (2 ** M)
        successor = find_successor(chord, node_id, finger_id)
        finger_table.append(successor)
    return finger_table


def print_finger_tables(chord):
    for node_id in chord:
        print(f"Node {node_id}\t finger table {get_finger_table(chord, node_id)}")


# Given Chord network
chord = [2, 16, 24, 25, 26, 31]

# Print finger tables for each node
print_finger_tables(chord)
