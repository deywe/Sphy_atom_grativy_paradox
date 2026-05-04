"""
SPHY Force Gradient Visualizer + SHA256 Auditor - Harpia Quantum
================================================================
3D visualizer of the 4 fundamental forces with quantum tunneling.
Includes real-time SHA256 validator for Parquet dataset auditing.

Forces simulated:
  [RED]  Strong Force  (range ~3 units,  octave 8.0 Hz)
  [GRN]  EM Force      (range ~7 units,  octave 2.0 Hz)
  [BLU]  Gravity       (range ~15 units, octave 0.5 Hz)
  [YLW]  Electron tunneling (crosses potential barrier)
"""

from ursina import *
from ursina.prefabs.trail_renderer import TrailRenderer
import numpy as np
import pandas as pd
import hashlib
import json
import math
import os
import threading
from pathlib import Path

# ─────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────
app = Ursina(
    title='SPHY Force Gradient Simulator - Harpia Quantum',
    borderless=False,
    fullscreen=False,
    size=(1920, 1080),
    vsync=True,
)

window.title = 'SPHY Force Gradient Simulator - Harpia Quantum'
window.color = color.black

# ─────────────────────────────────────────────
# SPHY PARAMETERS
# ─────────────────────────────────────────────
OCTAVES = {
    'GRAVITY': 0.5,
    'EM': 2.0,
    'STRONG': 8.0,
    'WEAK': 4.0,
}

simbiotic_phase = 0.0
DATASET_PATH = Path("sphy_dataset/sphy_simulation.parquet")
MANIFEST_PATH = Path("sphy_dataset/sphy_manifest.json")

# ─────────────────────────────────────────────
# AUDITOR GLOBAL STATE
# ─────────────────────────────────────────────
audit_state = {
    'running': False,
    'progress': 0.0,
    'current_frame': 0,
    'total_frames': 0,
    'passed': 0,
    'failed': 0,
    'status': 'Waiting for dataset...',
    'dataset_hash': '',
    'manifest_hash': '',
    'dataset_loaded': False,
    'audit_complete': False,
    'frame_results': [],
}

# ─────────────────────────────────────────────
# 3D SCENE
# ─────────────────────────────────────────────
EditorCamera()
PointLight(parent=camera, position=(0, 10, -10))
AmbientLight(color=color.rgba(30, 30, 60, 255))

# Unified field core
core       = Entity(model='sphere', color=color.cyan,  scale=1.5)
core_glow  = Entity(model='sphere', color=color.cyan,  scale=1.7, alpha=0.08)
core_glow2 = Entity(model='sphere', color=color.white, scale=1.2, alpha=0.15)

# Force range spheres (wireframe)
range_strong = Entity(model='sphere', color=color.red,   scale=6,  wireframe=True, alpha=0.05)
range_em     = Entity(model='sphere', color=color.green, scale=14, wireframe=True, alpha=0.03)
range_grav   = Entity(model='sphere', color=color.blue,  scale=30, wireframe=True, alpha=0.015)

# Field mesh
GRID_SIZE = 20
GRID_STEP = 2
dots = []
for x in range(-GRID_SIZE, GRID_SIZE, GRID_STEP):
    for z in range(-GRID_SIZE, GRID_SIZE, GRID_STEP):
        dot = Entity(model='sphere', position=(x, 0, z), scale=0.1, color=color.white33)
        dots.append(dot)

# Tunneling electron
electron = Entity(model='sphere', color=color.yellow, scale=0.4, position=(-15, 0, 0))
electron.trail = TrailRenderer(parent=electron, thickness=0.15, color=color.yellow, length=15)

# Weak force particle (beta decay orbit)
weak_particle = Entity(model='sphere', color=color.orange, scale=0.25, position=(0, 3, 0))

# ─────────────────────────────────────────────
# TEXT INTERFACE
# ─────────────────────────────────────────────
Text(text='Harpia Quantum -- SPHY Gravitational Unification',
     position=(-0.90, 0.47), scale=1.6, color=color.cyan)

Text(text='-- Fundamental Forces --',
     position=(-0.90, 0.41), scale=1.1, color=color.white)

Text(text='[RED]  Strong Force  | range < 3u  | octave 8.0 Hz | Quarks / Nucleon',
     position=(-0.90, 0.37), scale=0.95, color=color.red)

Text(text='[GRN]  EM Force      | range < 7u  | octave 2.0 Hz | Photons / Electrons',
     position=(-0.90, 0.33), scale=0.95, color=color.green)

Text(text='[BLU]  Gravity       | range < 15u | octave 0.5 Hz | Spacetime Curvature',
     position=(-0.90, 0.29), scale=0.95, color=color.azure)

Text(text='[ORG]  Weak Force    | range < 2u  | octave 4.0 Hz | Beta Decay / W+- Z0',
     position=(-0.90, 0.25), scale=0.95, color=color.orange)

Text(text='[YLW]  Electron      | Quantum Tunneling | crosses potential barrier',
     position=(-0.90, 0.21), scale=0.95, color=color.yellow)

Text(text='-- Live Metrics --',
     position=(-0.90, 0.14), scale=1.0, color=color.white)

phase_text    = Text(text='Symbiotic Phase : 0.000',   position=(-0.90,  0.10), scale=0.9, color=color.cyan)
electron_text = Text(text='Electron X      : -15.00',  position=(-0.90,  0.06), scale=0.9, color=color.yellow)
tunnel_text   = Text(text='Tunneling       : INACTIVE', position=(-0.90,  0.02), scale=0.9, color=color.white)
core_text     = Text(text='Core Scale      : 1.500',   position=(-0.90, -0.02), scale=0.9, color=color.cyan)

# ── SHA256 Auditor panel ──
Text(text='-- SHA256 Auditor --',
     position=(0.30, 0.47), scale=1.1, color=color.lime, origin=(0, 0))

audit_status_text   = Text(text='Status: Waiting for dataset...',
                            position=(0.30, 0.43), scale=0.9, color=color.white, origin=(0, 0))
audit_progress_text = Text(text='Progress: 0/0 frames',
                            position=(0.30, 0.39), scale=0.9, color=color.white, origin=(0, 0))
audit_passed_text   = Text(text='OK  Valid  : 0',
                            position=(0.30, 0.35), scale=0.9, color=color.lime, origin=(0, 0))
audit_failed_text   = Text(text='ERR Invalid: 0',
                            position=(0.30, 0.31), scale=0.9, color=color.red,  origin=(0, 0))
audit_dataset_hash_text  = Text(text='Dataset  SHA256: --',
                                 position=(0.30, 0.27), scale=0.75, color=color.azure, origin=(0, 0))
audit_manifest_hash_text = Text(text='Manifest SHA256: --',
                                 position=(0.30, 0.23), scale=0.75, color=color.azure, origin=(0, 0))

def on_audit_click():
    if not audit_state['running']:
        start_audit_thread()

audit_button = Button(
    text='> AUDIT PARQUET',
    position=(0.30, 0.14),
    scale=(0.35, 0.06),
    color=color.lime,
    text_color=color.black,
    on_click=on_audit_click,
)

Text(text='Loads sphy_dataset/sphy_simulation.parquet\nand validates each frame against manifest SHA256',
     position=(0.30, 0.07), scale=0.75, color=color.gray, origin=(0, 0))

# ─────────────────────────────────────────────
# SHA256 AUDIT LOGIC
# ─────────────────────────────────────────────
def run_audit():
    state = audit_state
    state['running'] = True
    state['status'] = 'Loading dataset...'
    state['passed'] = 0
    state['failed'] = 0
    state['frame_results'] = []
    state['audit_complete'] = False

    if not DATASET_PATH.exists():
        state['status'] = f'ERROR: {DATASET_PATH} not found'
        state['running'] = False
        return

    if not MANIFEST_PATH.exists():
        state['status'] = f'ERROR: {MANIFEST_PATH} not found'
        state['running'] = False
        return

    state['status'] = 'Loading manifest...'
    try:
        with open(MANIFEST_PATH, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
    except Exception as e:
        state['status'] = f'ERROR manifest: {e}'
        state['running'] = False
        return

    state['status'] = 'Computing dataset SHA256...'
    try:
        df = pd.read_parquet(DATASET_PATH)
        state['dataset_loaded'] = True
        full_bytes = json.dumps(df.to_dict(orient='records'), sort_keys=True, ensure_ascii=False).encode('utf-8')
        computed_dataset_hash = hashlib.sha256(full_bytes).hexdigest()
        expected_dataset_hash = manifest.get('dataset_sha256', '')
        state['dataset_hash'] = computed_dataset_hash[:32] + '...'
        if computed_dataset_hash != expected_dataset_hash:
            state['status'] = 'WARNING: DATASET HASH MISMATCH!'
        else:
            state['status'] = 'Dataset OK -- Verifying frames...'
    except Exception as e:
        state['status'] = f'ERROR reading parquet: {e}'
        state['running'] = False
        return

    try:
        manifest_bytes = json.dumps(manifest, sort_keys=True).encode('utf-8')
        state['manifest_hash'] = hashlib.sha256(manifest_bytes).hexdigest()[:32] + '...'
    except Exception:
        state['manifest_hash'] = 'error'

    frame_entries = manifest.get('frames', [])
    state['total_frames'] = len(frame_entries)

    for i, entry in enumerate(frame_entries):
        frame_idx = entry['frame_idx']
        expected  = entry['sha256']
        frame_df  = df[df['frame_idx'] == frame_idx]

        if len(frame_df) == 0:
            state['failed'] += 1
            state['frame_results'].append((frame_idx, False))
        else:
            frame_bytes = json.dumps(frame_df.to_dict(orient='records'), sort_keys=True, ensure_ascii=False).encode('utf-8')
            ok = (hashlib.sha256(frame_bytes).hexdigest() == expected)
            if ok:
                state['passed'] += 1
            else:
                state['failed'] += 1
            state['frame_results'].append((frame_idx, ok))

        state['current_frame'] = i + 1
        state['progress']      = (i + 1) / state['total_frames']

    state['audit_complete'] = True
    if state['failed'] == 0:
        state['status'] = f'AUDIT COMPLETE -- {state["passed"]} valid frames!'
    else:
        state['status'] = f'WARNING: {state["failed"]} INVALID frames out of {state["total_frames"]}'
    state['running'] = False


def start_audit_thread():
    threading.Thread(target=run_audit, daemon=True).start()


# ─────────────────────────────────────────────
# UPDATE LOOP
# ─────────────────────────────────────────────
def update():
    global simbiotic_phase
    simbiotic_phase += time.dt * 2

    # Core
    core_s = 1.5 + math.sin(simbiotic_phase) * 0.05
    core.scale       = core_s
    core.rotation_y += 20 * time.dt
    core_glow.scale  = core_s * 1.1 + math.sin(simbiotic_phase * 3) * 0.03
    core_glow2.scale = 1.2 + math.cos(simbiotic_phase * 2) * 0.04

    # Field mesh
    for d in dots:
        dist = distance(d.position, core.position)
        if dist < 15:
            wave = math.sin(simbiotic_phase - dist * (10 / (dist + 0.1)))
            d.y = wave * (2 / (dist + 1))
            if dist < 3:
                d.color = color.red
            elif dist < 7:
                d.color = color.green
            else:
                d.color = color.blue
        else:
            d.y     = 0
            d.color = color.white33

    # Electron
    if electron.x < 15:
        electron.x += 5 * time.dt
    else:
        electron.x = -15

    dist_to_core = distance(electron.position, core.position)
    tunneling = dist_to_core < 4
    if tunneling:
        electron.y     = math.sin(simbiotic_phase * 10) * 0.5
        electron.color = color.yellow
        electron.scale = 0.4 + math.sin(simbiotic_phase * 20) * 0.15
        tunnel_text.text  = f'Tunneling       : ACTIVE  psi={math.sin(simbiotic_phase*10):.3f}'
        tunnel_text.color = color.yellow
    else:
        electron.y     = 0
        electron.scale = 0.4
        tunnel_text.text  = 'Tunneling       : INACTIVE'
        tunnel_text.color = color.gray

    # Weak force particle (beta decay orbit)
    weak_particle.position = Vec3(
        math.cos(simbiotic_phase * OCTAVES['WEAK']) * 2.5,
        math.sin(simbiotic_phase * OCTAVES['WEAK']) * 2.5,
        math.sin(simbiotic_phase * 1.3) * 1.5,
    )
    weak_particle.scale = 0.25 * (0.7 + 0.3 * math.sin(simbiotic_phase * OCTAVES['WEAK'] * 2))

    # Live metrics
    phase_text.text    = f'Symbiotic Phase : {simbiotic_phase:.3f}'
    electron_text.text = f'Electron X      : {electron.x:+.2f}'
    core_text.text     = f'Core Scale      : {core_s:.3f}'

    # Auditor UI
    state = audit_state
    audit_status_text.text = f'Status: {state["status"]}'

    if state['total_frames'] > 0:
        audit_progress_text.text = (
            f'Progress: {state["current_frame"]}/{state["total_frames"]} frames '
            f'({state["progress"]*100:.1f}%)'
        )
    else:
        audit_progress_text.text = 'Progress: waiting...'

    audit_passed_text.text = f'OK  Valid  : {state["passed"]}'
    audit_failed_text.text = f'ERR Invalid: {state["failed"]}'

    if state['dataset_hash']:
        audit_dataset_hash_text.text  = f'Dataset  SHA256: {state["dataset_hash"]}'
    if state['manifest_hash']:
        audit_manifest_hash_text.text = f'Manifest SHA256: {state["manifest_hash"]}'

    if state['failed'] > 0:
        audit_status_text.color = color.red
    elif state['audit_complete']:
        audit_status_text.color = color.lime
    elif state['running']:
        audit_status_text.color = color.yellow
    else:
        audit_status_text.color = color.white


# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("  SPHY Force Gradient Visualizer - Harpia Quantum")
    print("=" * 60)
    print()
    print("  Camera controls:")
    print("    Mouse drag   -> rotate")
    print("    Scroll       -> zoom")
    print("    Right click  -> pan")
    print()
    print("  Click '> AUDIT PARQUET' to validate the dataset")
    print("  Dataset expected at: sphy_dataset/sphy_simulation.parquet")
    print()
    app.run()
