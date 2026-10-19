"""Choisit le mode de démarrage (minimal / light / full) et le mémorise dans .mode.

Usage : python tools/choose_mode.py [minimal|light|full]
Sans argument, affiche un menu interactif. Le Makefile relit .mode pour que
`make up`, `make app`, `make down`... utilisent tous le même mode.
"""
import sys
from pathlib import Path

MODES = {
    "minimal": "Kafka x1, Zookeeper, TimescaleDB, Redis             (le plus léger)",
    "light": "minimal + Spark, MinIO, Kafka UI                      (1 broker, sans Schema Registry)",
    "full": "Kafka x3, Schema Registry, Spark, MinIO, Kafka UI ... (le plus lourd)",
}
MODE_FILE = Path(__file__).resolve().parent.parent / ".mode"


def ask() -> str:
    names = list(MODES)
    print("Choisissez un mode de démarrage :\n")
    for i, name in enumerate(names, 1):
        print(f"  {i}) {name:<8} {MODES[name]}")
    while True:
        answer = input("\nVotre choix [1-3, défaut 1] : ").strip().lower() or "1"
        if answer in MODES:
            return answer
        if answer.isdigit() and 1 <= int(answer) <= len(names):
            return names[int(answer) - 1]
        print("Choix invalide.")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else ask()
    if mode not in MODES:
        sys.exit(f"Mode inconnu : {mode} (attendu : {', '.join(MODES)})")
    MODE_FILE.write_text(f"MODE := {mode}\n", encoding="utf-8")
    print(f"Mode '{mode}' enregistré dans .mode")


if __name__ == "__main__":
    main()
