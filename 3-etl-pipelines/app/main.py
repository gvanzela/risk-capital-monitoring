import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
from datetime import datetime
import threading

# ============================================================
# JOB IMPORTS
# ============================================================

from app.jobs import (
    run_margem,
    run_pl_snapshot,
    run_pl_historico,
    run_posicoes,
    run_swaps,
    backup_local
)

# ============================================================
# THREAD HELPER (avoid UI freeze)
# ============================================================

def run_in_thread(func):
    threading.Thread(target=func).start()

# ============================================================
# APPLICATION
# ============================================================

class App:
    def __init__(self, root):
        self.root = root
        root.title("Risk Capital Jobs")
        root.geometry("500x550")

        # ----------------------------------------------------
        # DATE PICKER (PL HISTORICAL)
        # ----------------------------------------------------
        tk.Label(root, text="Reference date for PL Historical:").pack(pady=5)
        self.date_picker = DateEntry(root, width=18, date_pattern="yyyy-mm-dd")
        self.date_picker.pack(pady=5)

        # ----------------------------------------------------
        # BUTTONS
        # ----------------------------------------------------
        ttk.Button(
            root,
            text="Run Manager Margin",
            command=lambda: run_in_thread(lambda: run_margem(self.log))
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Run PL Snapshot",
            command=lambda: run_in_thread(lambda: run_pl_snapshot(self.log))
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Run PL Historical",
            command=lambda: run_in_thread(self.run_pl_hist_wrapper)
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Run Fund Positions",
            command=lambda: run_in_thread(lambda: run_posicoes(self.log))
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Run Swaps",
            command=lambda: run_in_thread(lambda: run_swaps(self.log))
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Local Backup (Remote → Local)",
            command=lambda: run_in_thread(lambda: backup_local(self.log))
        ).pack(fill="x", padx=20, pady=5)

        ttk.Button(
            root,
            text="Run ALL",
            command=lambda: run_in_thread(self.run_all)
        ).pack(fill="x", padx=20, pady=10)

        # ----------------------------------------------------
        # LOG BOX
        # ----------------------------------------------------
        tk.Label(root, text="Log:").pack()
        self.log_box = tk.Text(root, height=12, width=60)
        self.log_box.pack(padx=10, pady=10)

    # --------------------------------------------------------
    # LOG FUNCTION
    # --------------------------------------------------------
    def log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.insert(tk.END, f"[{ts}] {msg}\n")
        self.log_box.see(tk.END)

    # --------------------------------------------------------
    # WRAPPERS
    # --------------------------------------------------------
    def run_pl_hist_wrapper(self):
        data = self.date_picker.get()
        run_pl_historico(self.log, data)

    def run_all(self):
        data = self.date_picker.get()
        self.log("Starting full pipeline...")

        run_margem(self.log)
        run_pl_snapshot(self.log)
        run_pl_historico(self.log, data)
        run_posicoes(self.log)
        run_swaps(self.log)
        backup_local(self.log)

        self.log("Pipeline finished successfully.")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
