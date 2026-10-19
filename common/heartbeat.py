"""Heartbeat fichier pour les healthchecks Docker des services sans port HTTP.

Le service appelle beat() dans sa boucle principale ; le healthcheck lance
`python -m common.heartbeat 90` (code retour 0 si le fichier a moins de 90 s).
"""

import os
import sys
import time

PATH = os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat")
_last = 0.0


def beat(min_interval: float = 5.0) -> None:
    """Touche le fichier de heartbeat (au plus une fois toutes les `min_interval` secondes)."""
    global _last
    now = time.time()
    if now - _last < min_interval:
        return
    _last = now
    try:
        with open(PATH, "a"):
            os.utime(PATH, None)
    except OSError:
        pass


def check(max_age: float = 60.0) -> int:
    """0 si le heartbeat est récent, 1 sinon."""
    try:
        return 0 if time.time() - os.path.getmtime(PATH) < max_age else 1
    except OSError:
        return 1


if __name__ == "__main__":
    sys.exit(check(float(sys.argv[1]) if len(sys.argv) > 1 else 60.0))
