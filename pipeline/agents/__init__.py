"""
pipeline/agents — CrewAI агенты ShortsProject.

Иерархия:
  COMMANDER → DIRECTOR → [SCOUT, CURATOR, VISIONARY, NARRATOR,
                           EDITOR, STRATEGIST, GUARDIAN,
                           PUBLISHER, ACCOUNTANT, SENTINEL]

Агенты оркеструют существующие модули pipeline/*.py
и не дублируют их логику.
"""
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import GPUResourceManager, GPUPriority, get_gpu_manager
from pipeline.agent_memory import AgentMemory, get_memory

__all__ = [
    "BaseAgent", "AgentStatus",
    "GPUResourceManager", "GPUPriority", "get_gpu_manager",
    "AgentMemory", "get_memory",
]
