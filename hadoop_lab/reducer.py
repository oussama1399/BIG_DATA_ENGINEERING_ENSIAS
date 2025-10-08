# reducer.py (robuste)
import sys

current_word = None
current_count = 0

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue

    # try split on tab first, fallback to whitespace
    parts = line.split('\t', 1)
    if len(parts) == 2:
        word, count_str = parts
    else:
        parts = line.split()
        if len(parts) < 2:
            # ligne malformée -> ignorer
            continue
        word, count_str = parts[0], parts[1]

    try:
        count = int(count_str)
    except ValueError:
        # valeur non entière -> ignorer
        continue

    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# imprimer le dernier groupe
if current_word is not None:
    print(f"{current_word}\t{current_count}")