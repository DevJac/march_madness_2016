import os


for filename in os.listdir('data'):
    with open(os.path.join('data', filename)) as f:
        old_lines = f.readlines()
    with open(os.path.join('data', filename), 'w') as f:
        for line_number, line in enumerate(old_lines, 1):
            if line_number not in [1, 23, 24]:
                f.write(line)
