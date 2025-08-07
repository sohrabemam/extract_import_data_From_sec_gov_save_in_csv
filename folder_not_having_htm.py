"""import os

base_dir = 'downloads'  # or your actual base folder path
folders_without_htm = []

for folder_name in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, folder_name)
    if os.path.isdir(folder_path):
        has_htm = any(
            file.endswith('.htm') or file.endswith('.html')
            for file in os.listdir(folder_path)
        )
        if not has_htm:
            folders_without_htm.append(folder_name)

print(f"🔍 Folders without any .htm files ({len(folders_without_htm)}):")
for folder in folders_without_htm:
    print(folder)
"""

import os

base_dir = 'downloads'  # change this to your actual folder
empty_folders = []

for folder_name in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, folder_name)
    if os.path.isdir(folder_path):
        if not os.listdir(folder_path):  # checks if the folder is empty
            empty_folders.append(folder_name)

print(f"🗂️ Empty folders found: {len(empty_folders)}")
for folder in empty_folders:
    print(folder)
