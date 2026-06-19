import json
from collections import deque, defaultdict

def get_detailed_levels(graph):
  # Step 1: Calculate in-degrees
  in_degree = {u: 0 for u in graph}
  for u in graph:
    for v in graph[u]:
      in_degree[v] = in_degree.get(v, 0) + 1
  # Step 2: Track parents for every node
  # (Inverse of the adjacency list)
  parents_map = defaultdict(list)
  for parent, children in graph.items():
    for child in children:
      parents_map[child].append(parent)
  # Step 3: Find source nodes (Level 0)
  queue = deque()
  for u in in_degree:
    if in_degree[u] == 0:
      queue.append((u, 0))
  # Step 4: Process levels
  raw_levels = defaultdict(list)
  while queue:
    node, level = queue.popleft()
    raw_levels[level].append(node)
    for neighbor in graph[node]:
      in_degree[neighbor] -= 1
      if in_degree[neighbor] == 0:
        queue.append((neighbor, level + 1))
  # Step 5: Build the final structured output
  structured_levels = {}
  for level_idx in sorted(raw_levels.keys()):
    # Sort nodes alphabetically within the level
    sorted_nodes = sorted(raw_levels[level_idx])
    level_nodes_info = []
    for node in sorted_nodes:
      # Gather connections for this specific node
      node_info = {
        "node": node,
        # "parents": sorted(parents_map[node]),  # Who points to me
        "children": sorted(graph[node])        # Who I point to
      }
      level_nodes_info.append(node_info)
    structured_levels[f"{level_idx+1}"] = level_nodes_info
  return structured_levels

if __name__ in '__main__':
  dag = {
    'Level1': ['DataCollectionModule', 'EvidencesCollectionModule'],
    'DataCollectionModule': ['Level3'],
    'EvidencesCollectionModule': ['Level3'],
    'Level3': ['PepModule', 'SanctionsModule'],
    'PepModule': [],
    'SanctionsModule': [],
  }
  level_structure = get_detailed_levels(dag)
  print(json.dumps(level_structure, indent=2))

"""
{
  "1": [
    {
      "node": "Level1",
      "children": [
        "DataCollectionModule",
        "EvidencesCollectionModule"
      ]
    }
  ],
  "2": [
    {
      "node": "DataCollectionModule",
      "children": [
        "Level3"
      ]
    },
    {
      "node": "EvidencesCollectionModule",
      "children": [
        "Level3"
      ]
    }
  ],
  "3": [
    {
      "node": "Level3",
      "children": [
        "PepModule",
        "SanctionsModule"
      ]
    }
  ],
  "4": [
    {
      "node": "PepModule",
      "children": []
    },
    {
      "node": "SanctionsModule",
      "children": []
    }
  ]
}
"""
