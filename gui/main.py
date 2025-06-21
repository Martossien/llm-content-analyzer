import sys
from pathlib import Path
from tkinter import Tk

# Ajouter le répertoire parent au path pour les imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gui.main_window import MainWindow


def main():
    root = Tk()
    root.geometry("1200x800")
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()