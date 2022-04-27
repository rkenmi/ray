import functools
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Union

from ray.autoscaler._private.cli_logger import cli_logger


class States(Enum):
    UNKNOWN = None
    NEW = 1
    DISPATCHED = 2
    STARTED = 3
    IN_PROGRESS = 4
    COMPLETED = 5


class CreateClusterEvent(Enum):
    """Events to track in ray.autoscaler.sdk.create_or_update_cluster.

    Attributes:
        up_started : Invoked at the beginning of create_or_update_cluster.
        ssh_keypair_downloaded : Invoked when the ssh keypair is downloaded.
        cluster_booting_started : Invoked when when the cluster booting starts.
        acquiring_new_head_node : Invoked before the head node is acquired.
        head_node_acquired : Invoked after the head node is acquired.
        ssh_control_acquired : Invoked when the node is being updated.
        run_initialization_cmd : Invoked before all initialization
            commands are called and again before each initialization command.
        run_setup_cmd : Invoked before all setup commands are
            called and again before each setup command.
        start_ray_runtime : Invoked before ray start commands are run.
        start_ray_runtime_completed : Invoked after ray start commands
            are run.
        cluster_booting_completed : Invoked after cluster booting
            is completed.
    """
    @property
    def state(self) -> str:
        return States.DISPATCHED.name

    up_started = auto()
    ssh_keypair_downloaded = auto()
    cluster_booting_started = auto()
    acquiring_new_head_node = auto()
    head_node_acquired = auto()
    ssh_control_acquired = auto()
    run_initialization_cmd = auto()
    run_setup_cmd = auto()
    start_ray_runtime = auto()
    start_ray_runtime_completed = auto()
    cluster_booting_completed = auto()


class ScriptStartedEvent(Enum):
    """Events to track for Ray scripts that are executed.
    """
    @property
    def state(self) -> str:
        return States.STARTED.name

    start_initializing = auto()


class ScriptInProgressEvent(Enum):
    """Events to track during execution of Ray scripts.
    """
    @property
    def state(self) -> str:
        return States.IN_PROGRESS.name

    in_progress = auto()


class ScriptInProgressCustomEvent:
    """Custom, user-defined events to track during execution of Ray scripts.
    """
    @property
    def state(self) -> str:
        return States.IN_PROGRESS.name

    @property
    def name(self) -> str:
        return self.event_name

    @property
    def value(self) -> int:
        # the state sequence number in 1-based indexing
        return self.state_sequence + 1

    def __init__(self, event_name: str, state_sequence: int):
        self.event_name = event_name
        self.state_sequence = state_sequence


class ScriptCompletedEvent(Enum):
    """Events to track for Ray scripts that are executed.
    """
    @property
    def state(self) -> str:
        return States.COMPLETED.name

    complete_success = auto()


RayEvent = Union[CreateClusterEvent, ScriptStartedEvent, ScriptInProgressEvent, ScriptInProgressCustomEvent,
                 ScriptCompletedEvent]
event_enums = [CreateClusterEvent, ScriptStartedEvent, ScriptInProgressEvent, ScriptCompletedEvent]
event_enum_values = [sequence for event in event_enums
                     for sequence in event.__members__.values()]


class _EventSystem:
    """Event system that handles storing and calling callbacks for events.

    Attributes:
        callback_map (Dict[str, List[Callable]]) : Stores list of callbacks
            for events when registered.
    """

    def __init__(self):
        self.callback_map = {}

    def add_callback_handler(
        self,
        event: str,
        callback: Union[Callable[[Dict], None], List[Callable[[Dict], None]]],
        *args,
        **kwargs
    ):
        """Stores callback handler for event.

        Args:
            event (str): Event that callback should be called on. See
                CreateClusterEvent for details on the events available to be
                registered against.
            callback (Callable[[Dict], None]): Callable object that is invoked
                when specified event occurs.
            *args: Variable length arguments to be injected into callbacks.
            **kwargs: Keyword arguments to be injected into callbacks.
        """
        if event not in event_enum_values:
            cli_logger.warning(
                f"{event} is not currently tracked, and this"
                " callback will not be invoked."
            )

        callback = functools.partial(callback, *args, **kwargs)
        self.callback_map.setdefault(event, []).extend(
            [callback] if type(callback) is not list else callback
        )

    def execute_callback(
        self, event: RayEvent, event_data: Optional[Dict[str, Any]] = None
    ):
        """Executes all callbacks for event.

        Args:
            event (str): Event that is invoked. See CreateClusterEvent
                for details on the available events.
            event_data (Dict[str, Any]): Argument that is passed to each
                callable object stored for this particular event.
        """
        if event_data is None:
            event_data = {}

        event_data["event"] = event
        if event in self.callback_map:
            for callback in self.callback_map[event]:
                callback(event_data)

    def clear_callbacks_for_event(self, event: str):
        """Clears stored callable objects for event.

        Args:
            event (str): Event that has callable objects stored in map.
                See CreateClusterEvent for details on the available events.
        """
        if event in self.callback_map:
            del self.callback_map[event]


global_event_system = _EventSystem()
