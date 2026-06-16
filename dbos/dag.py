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
        "parents": sorted(parents_map[node]),  # Who points to me
        "children": sorted(graph[node])        # Who I point to
      }
      level_nodes_info.append(node_info)
    structured_levels[f"Level {level_idx}"] = level_nodes_info
  return structured_levels

if __name__ in '__main__':
  dag = {
    'main.MainWorkflow': ['main.MainWorkflowChildPhase1'],
    'main.MainWorkflowChildPhase1': ['main.DataCollectionStep', 'main.EvidencesCollectionStep'],
    'main.DataCollectionStep': ['main.MainWorkflowChildPhase2'],
    'main.EvidencesCollectionStep': ['main.MainWorkflowChildPhase2'],
    'main.MainWorkflowChildPhase2': ['main.PepModuleStep', 'main.SanctionsModuleStep'],
    'main.PepModuleStep': [],
    'main.SanctionsModuleStep': [],
  }
  level_structure = get_detailed_levels(dag)
  print(json.dumps(level_structure, indent=2))
  
"""
{
  "Level 0": [
    {
      "node": "main.MainWorkflow",
      "parents": [],
      "children": [
        "main.MainWorkflowChildPhase1"
      ]
    }
  ],
  "Level 1": [
    {
      "node": "main.MainWorkflowChildPhase1",
      "parents": [
        "main.MainWorkflow"
      ],
      "children": [
        "main.DataCollectionStep",
        "main.EvidencesCollectionStep"
      ]
    }
  ],
  "Level 2": [
    {
      "node": "main.DataCollectionStep",
      "parents": [
        "main.MainWorkflowChildPhase1"
      ],
      "children": [
        "main.MainWorkflowChildPhase2"
      ]
    },
    {
      "node": "main.EvidencesCollectionStep",
      "parents": [
        "main.MainWorkflowChildPhase1"
      ],
      "children": [
        "main.MainWorkflowChildPhase2"
      ]
    }
  ],
  "Level 3": [
    {
      "node": "main.MainWorkflowChildPhase2",
      "parents": [
        "main.DataCollectionStep",
        "main.EvidencesCollectionStep"
      ],
      "children": [
        "main.PepModuleStep",
        "main.SanctionsModuleStep"
      ]
    }
  ],
  "Level 4": [
    {
      "node": "main.PepModuleStep",
      "parents": [
        "main.MainWorkflowChildPhase2"
      ],
      "children": []
    },
    {
      "node": "main.SanctionsModuleStep",
      "parents": [
        "main.MainWorkflowChildPhase2"
      ],
      "children": []
    }
  ]
}
"""
