import os
from config.io_config import *
from datetime import datetime

class RuntimeContext:
    """
    Manage shared runtime context across notebooks/modules.
    """

    RUN_ID_FILE = os.path.join(LOG_DIR, "etl_run_id.txt")

    @staticmethod
    def generate_run_id() -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    @classmethod
    def set_run_id(cls, run_id: str) -> None:
        """
        Persist run_id to shared storage (DBFS)
        """
        os.makedirs(os.path.dirname(cls.RUN_ID_FILE), exist_ok=True)
        with open(cls.RUN_ID_FILE, "w") as f:
            f.write(run_id)

    @classmethod
    def get_run_id(cls) -> str:
        """
        Get run_id from shared storage, fallback to generate
        """
        try:
            with open(cls.RUN_ID_FILE, "r") as f:
                return f.read().strip()
        except:
            return cls.generate_run_id()
