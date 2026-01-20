#this script lists all files in its current directory, 
#and displays the contents of each file except itself.

from pathlib import Path

# Get the current directory and the name of this script
current_dir = Path.cwd()
current_file = Path(__file__).name

# Print the list of files in the current directory
print(f"Files in {current_dir}:")

# Iterate through each file in the directory and display its content 
# but skips itself
for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue

    print(f"  - {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        print(f"    Content: {content}")