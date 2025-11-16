import pyautogui as pag
import keyboard
import threading
import time
import tkinter as tk
from tkinter import ttk

running = False

def autoclick():
    global running
    while running:
        if keyboard.is_pressed("q"):
            running = False
            break
        pag.click()
        time.sleep(speed.get() / 1000)

def start_clicking():
    global running
    if not running:
        running = True
        threading.Thread(target=autoclick, daemon=True).start()
        status_label.config(text="Статус: работает (нажми Q чтобы остановить)", foreground="green")

def stop_clicking():
    global running
    running = False
    status_label.config(text="Статус: остановлено", foreground="red")

# GUI
root = tk.Tk()
root.title("AutoClicker by Belek")
root.geometry("350x220")
root.resizable(False, False)

style = ttk.Style()
style.configure("TButton", font=("Arial", 12))

ttk.Label(root, text="Скорость кликов (мс):", font=("Arial", 12)).pack(pady=5)

speed = tk.IntVar(value=10)
speed_entry = ttk.Entry(root, textvariable=speed, font=("Arial", 12))
speed_entry.pack(pady=5)

ttk.Button(root, text="Старт", command=start_clicking).pack(pady=8)
ttk.Button(root, text="Стоп", command=stop_clicking).pack(pady=5)

status_label = ttk.Label(root, text="Статус: остановлено", font=("Arial", 11), foreground="red")
status_label.pack(pady=10)

ttk.Label(root, text="Хоткей: Q — остановить", font=("Arial", 10)).pack(pady=2)

root.mainloop()
