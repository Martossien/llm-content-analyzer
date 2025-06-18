from tkinter import Tk
from .main_window import MainWindow


def main():
    root = Tk()
    root.geometry("1200x800")
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
