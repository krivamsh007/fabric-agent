"""
Base Agent Class
================

Foundation for all specialized agents in the multi-agent system.

Provides:
- LLM integration (Claude/GPT)
- Tool calling framework
- Shared memory access
- Message passing between agents

Author: Fabric Agent Team
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, Union
from uuid import uuid4

from loguru import logger


# =============================================================================
# Data Models
# =============================================================================

class AgentRole(str, Enum):
    """Roles for different agents."""
    DISCOVERY = "discovery"
    IMPACT = "impact"
    REFACTOR = "refactor"
    VALIDATION = "validation"
    ORCHESTRATOR = "orchestrator"


class MessageType(str, Enum):
    """Types of messages between agents."""
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"
    ERROR = "error"


class TaskStatus(str, Enum):
    """Status of an agent task."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AgentMessage:
    """Message passed between agents."""
    id: str
    from_agent: str
    to_agent: str
    message_type: MessageType
    content: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: Optional[str] = None  # For tracking related messages
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "message_type": self.message_type.value,
            "content": self.content,
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
        }


@dataclass
class ToolDefinition:
    """Definition of a tool available to agents."""
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema
    handler: Callable
    
    def to_llm_format(self) -> Dict[str, Any]:
        """Convert to format expected by LLM APIs."""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": {
                "type": "object",
                "properties": self.parameters,
            }
        }


@dataclass
class ToolCall:
    """A tool call made by an agent."""
    tool_name: str
    arguments: Dict[str, Any]
    result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class AgentThought:
    """A thought/reasoning step from an agent."""
    step: int
    thought: str
    action: Optional[str] = None
    observation: Optional[str] = None


@dataclass
class AgentResult:
    """Result of an agent execution."""
    agent_name: str
    task: str
    status: TaskStatus
    result: Any
    thoughts: List[AgentThought] = field(default_factory=list)
    tool_calls: List[ToolCall] = field(default_factory=list)
    error: Optional[str] = None
    execution_time_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "task": self.task,
            "status": self.status.value,
            "result": self.result,
            "thoughts": [vars(t) for t in self.thoughts],
            "tool_calls": [vars(tc) for tc in self.tool_calls],
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
        }


# =============================================================================
# Shared Memory
# =============================================================================

class SharedMemory:
    """
    Shared memory accessible by all agents.
    
    Stores:
    - Conversation history
    - Task results
    - Workspace state
    - Cached data
    """
    
    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._history: List[AgentMessage] = []
        self._results: Dict[str, AgentResult] = {}
    
    def set(self, key: str, value: Any) -> None:
        """Store a value."""
        self._store[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value."""
        return self._store.get(key, default)
    
    def delete(self, key: str) -> None:
        """Delete a value."""
        self._store.pop(key, None)
    
    def add_message(self, message: AgentMessage) -> None:
        """Add a message to history."""
        self._history.append(message)
    
    def get_history(self, limit: int = 100) -> List[AgentMessage]:
        """Get recent message history."""
        return self._history[-limit:]
    
    def store_result(self, task_id: str, result: AgentResult) -> None:
        """Store a task result."""
        self._results[task_id] = result
    
    def get_result(self, task_id: str) -> Optional[AgentResult]:
        """Retrieve a task result."""
        return self._results.get(task_id)
    
    def clear(self) -> None:
        """Clear all memory."""
        self._store.clear()
        self._history.clear()
        self._results.clear()


# =============================================================================
# LLM Client Interface
# =============================================================================

class LLMClient(ABC):
    """Abstract interface for LLM providers."""
    
    @abstractmethod
    async def complete(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send completion request to LLM."""
        pass


class ClaudeLLMClient(LLMClient):
    """Claude (Anthropic) LLM client."""
    
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6"):
        self.api_key = api_key
        self.model = model
        self._client = None
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send completion request to Claude."""
        try:
            import anthropic
            
            if self._client is None:
                self._client = anthropic.AsyncAnthropic(api_key=self.api_key)
            
            kwargs = {
                "model": self.model,
                "max_tokens": 4096,
                "messages": messages,
            }
            
            if system_prompt:
                kwargs["system"] = system_prompt
            
            if tools:
                kwargs["tools"] = tools
            
            response = await self._client.messages.create(**kwargs)
            
            return {
                "content": response.content,
                "stop_reason": response.stop_reason,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }
            }
            
        except ImportError:
            logger.error("anthropic package not installed")
            raise RuntimeError("Install anthropic: pip install anthropic")


class MockLLMClient(LLMClient):
    """Mock LLM client for testing without API calls."""
    
    def __init__(self, responses: Optional[List[Dict]] = None):
        self.responses = responses or []
        self.call_count = 0
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return mock response."""
        if self.responses and self.call_count < len(self.responses):
            response = self.responses[self.call_count]
            self.call_count += 1
            return response
        
        # Default mock response
        return {
            "content": [{"type": "text", "text": "Mock response"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 100, "output_tokens": 50}
        }


# =============================================================================
# Base Agent
# =============================================================================

class BaseAgent(ABC):
    """
    Base class for all agents.
    
    Implements the agent loop:
    1. Receive task
    2. Think (using LLM)
    3. Act (call tools)
    4. Observe (process results)
    5. Repeat until done
    
    Subclasses implement:
    - get_system_prompt()
    - get_tools()
    - process_result() (optional)
    """
    
    def __init__(
        self,
        name: str,
        role: AgentRole,
        llm: LLMClient,
        memory: SharedMemory,
        max_iterations: int = 10,
    ):
        """
        Initialize the agent.
        
        Args:
            name: Unique agent name
            role: Agent's role
            llm: LLM client for reasoning
            memory: Shared memory
            max_iterations: Max think-act cycles
        """
        self.name = name
        self.role = role
        self.llm = llm
        self.memory = memory
        self.max_iterations = max_iterations
        self._tools: Dict[str, ToolDefinition] = {}
        
        # Register tools
        for tool in self.get_tools():
            self._tools[tool.name] = tool
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Return the system prompt for this agent."""
        pass
    
    @abstractmethod
    def get_tools(self) -> List[ToolDefinition]:
        """Return the tools available to this agent."""
        pass
    
    async def run(
        self,
        task: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> AgentResult:
        """
        Execute the agent on a task.
        
        Args:
            task: The task to perform
            context: Additional context
        
        Returns:
            AgentResult with outcome
        """
        import time
        start_time = time.time()
        
        logger.info(f"[{self.name}] Starting task: {task[:100]}...")
        
        thoughts: List[AgentThought] = []
        tool_calls: List[ToolCall] = []
        
        # Build initial messages
        messages = [
            {"role": "user", "content": self._build_task_prompt(task, context)}
        ]
        
        # Get tools in LLM format
        llm_tools = [t.to_llm_format() for t in self._tools.values()] if self._tools else None
        
        try:
            for iteration in range(self.max_iterations):
                # Call LLM
                response = await self.llm.complete(
                    messages=messages,
                    tools=llm_tools,
                    system_prompt=self.get_system_prompt(),
                )
                
                # Process response
                content = response.get("content", [])
                stop_reason = response.get("stop_reason", "")
                
                # Extract text and tool use
                text_content = ""
                tool_use_blocks = []
                
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            text_content += block.get("text", "")
                        elif block.get("type") == "tool_use":
                            tool_use_blocks.append(block)
                    elif hasattr(block, "type"):
                        if block.type == "text":
                            text_content += block.text
                        elif block.type == "tool_use":
                            tool_use_blocks.append({
                                "id": block.id,
                                "name": block.name,
                                "input": block.input,
                            })
                
                # Record thought
                thought = AgentThought(
                    step=iteration + 1,
                    thought=text_content[:500] if text_content else "Processing...",
                )
                thoughts.append(thought)
                
                # If no tool calls, we're done
                if not tool_use_blocks or stop_reason == "end_turn":
                    logger.info(f"[{self.name}] Completed after {iteration + 1} iterations")
                    
                    execution_time = int((time.time() - start_time) * 1000)
                    
                    return AgentResult(
                        agent_name=self.name,
                        task=task,
                        status=TaskStatus.COMPLETED,
                        result=self._extract_result(text_content, context),
                        thoughts=thoughts,
                        tool_calls=tool_calls,
                        execution_time_ms=execution_time,
                    )
                
                # Execute tool calls
                tool_results = []
                for tool_block in tool_use_blocks:
                    tool_name = tool_block.get("name", "")
                    tool_input = tool_block.get("input", {})
                    tool_id = tool_block.get("id", str(uuid4()))
                    
                    logger.debug(f"[{self.name}] Calling tool: {tool_name}")
                    
                    tool_call = ToolCall(tool_name=tool_name, arguments=tool_input)
                    
                    if tool_name in self._tools:
                        try:
                            result = await self._call_tool(tool_name, tool_input)
                            tool_call.result = result
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": tool_id,
                                "content": json.dumps(result) if not isinstance(result, str) else result,
                            })
                        except Exception as e:
                            tool_call.error = str(e)
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": tool_id,
                                "content": f"Error: {str(e)}",
                                "is_error": True,
                            })
                    else:
                        tool_call.error = f"Unknown tool: {tool_name}"
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": f"Unknown tool: {tool_name}",
                            "is_error": True,
                        })
                    
                    tool_calls.append(tool_call)
                    thought.action = f"Called {tool_name}"
                    thought.observation = str(tool_call.result)[:200] if tool_call.result else tool_call.error
                
                # Add assistant response and tool results to messages
                messages.append({"role": "assistant", "content": content})
                messages.append({"role": "user", "content": tool_results})
            
            # Max iterations reached
            logger.warning(f"[{self.name}] Max iterations ({self.max_iterations}) reached")
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return AgentResult(
                agent_name=self.name,
                task=task,
                status=TaskStatus.COMPLETED,
                result=self._extract_result(text_content, context),
                thoughts=thoughts,
                tool_calls=tool_calls,
                execution_time_ms=execution_time,
            )
            
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return AgentResult(
                agent_name=self.name,
                task=task,
                status=TaskStatus.FAILED,
                result=None,
                thoughts=thoughts,
                tool_calls=tool_calls,
                error=str(e),
                execution_time_ms=execution_time,
            )
    
    async def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Execute a tool."""
        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")
        
        # Call handler (may be sync or async)
        result = tool.handler(**arguments)
        if asyncio.iscoroutine(result):
            result = await result
        
        return result
    
    def _build_task_prompt(self, task: str, context: Optional[Dict]) -> str:
        """Build the task prompt with context."""
        prompt = f"Task: {task}"
        
        if context:
            prompt += f"\n\nContext:\n{json.dumps(context, indent=2)}"
        
        return prompt
    
    def _extract_result(self, text: str, context: Optional[Dict]) -> Any:
        """Extract structured result from LLM response. Override in subclasses."""
        return {"response": text}
    
    def send_message(self, to_agent: str, content: Dict[str, Any]) -> AgentMessage:
        """Send a message to another agent."""
        message = AgentMessage(
            id=str(uuid4()),
            from_agent=self.name,
            to_agent=to_agent,
            message_type=MessageType.REQUEST,
            content=content,
        )
        self.memory.add_message(message)
        return message


# =============================================================================
# Simple Agent (No LLM, just tools)
# =============================================================================

class SimpleAgent(BaseAgent):
    """
    A simple agent that executes tools directly without LLM reasoning.

    Useful for deterministic operations that don't need AI reasoning.
    """

    def __init__(
        self,
        name: str,
        role: AgentRole,
        memory: SharedMemory,
        tools: List[ToolDefinition],
    ):
        # IMPORTANT:
        # BaseAgent.__init__ registers tools by calling self.get_tools().
        # SimpleAgent.get_tools() returns self._simple_tools, so we must
        # set it *before* calling super().__init__.
        self._simple_tools = tools

        # Use mock LLM since we won't use it
        super().__init__(
            name=name,
            role=role,
            llm=MockLLMClient(),
            memory=memory,
            max_iterations=1,
        )

        # BaseAgent already registered tools via get_tools(), but keep this
        # to ensure tools are present even if BaseAgent behavior changes.
        for tool in tools:
            self._tools[tool.name] = tool

    def get_system_prompt(self) -> str:
        return ""

    def get_tools(self) -> List[ToolDefinition]:
        return self._simple_tools

    async def execute_tool(self, tool_name: str, **kwargs) -> Any:
        """Directly execute a tool without LLM."""
        return await self._call_tool(tool_name, kwargs)
