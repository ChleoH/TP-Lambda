import shutil
import os
from datetime import datetime

source_dir = 'data/speed_layer/'
target_dir = f'data/merged/{datetime.now().strftime("%Y-%m-%d-%H%M")}'

os.makedirs(target_dir, exist_ok=True)

for file in os.listdir(source_dir):
    if file.endswith(".json"):
        shutil.move(os.path.join(source_dir, file), target_dir)
