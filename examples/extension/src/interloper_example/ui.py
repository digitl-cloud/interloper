"""The example's UI, which ``pnpm --filter @interloper/example build`` writes into ``static/``."""

from pathlib import Path

from interloper_api import UIBundle

UI = UIBundle.from_vite(Path(__file__).parent / "static")
