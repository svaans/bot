from typing import Dict


class BotRunner:
    """Lanza y detiene tareas asincrónicas que ejecutan el bot."""

    def __init__(self) ->None:
        self._tasks: Dict[int, object] = {}

    def start(self, user_id: int) ->bool:
        """Marca la tarea como iniciada."""
        if user_id in self._tasks:
            return False
        self._tasks[user_id] = object()
        return True

    def stop(self, user_id: int) ->None:
        self._tasks.pop(user_id, None)

    def is_running(self, user_id: int) ->bool:
        return user_id in self._tasks


bot_runner = BotRunner()
