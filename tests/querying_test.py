import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from database import query_with_conditions, query_option_chain

results = query_option_chain('2025-05-23', '16:35', 'P', 5770)
print(results)

results = query_with_conditions('2025-05-23', '2025-05-24', '15:31:00', '20:00:00', '')
print(results)