# Music Library Manager

A set of Python tools to catalog, organize, and deduplicate a music library.

## Features

- **Catalog** - Scans music files and extracts metadata (artist, album, title, duration, bitrate) into a SQLite database
- **Organize** - Standardizes folder and file naming conventions
- **Consolidate** - Moves incomplete albums and singles into "Artist - Mixed" folders
- **Deduplicate** - Identifies and removes duplicate tracks
- **Partial Detection** - Finds albums with missing tracks based on track number metadata

## Requirements

- Python 3.6+
- mutagen library for audio metadata extraction

```bash
pip install mutagen
```

## Files

| File | Description |
|------|-------------|
| `music_catalog.py` | Scans library, manages database, finds duplicates |
| `music_organizer.py` | Reorganizes folder structure, removes duplicates |
| `music_catalog.db` | SQLite database containing the catalog |

## Usage

### Cataloging Your Library

```bash
# Initial scan (or after adding new music)
python music_catalog.py --scan

# Force full rescan (re-reads all metadata)
python music_catalog.py --rescan

# Show library statistics
python music_catalog.py --stats

# Find all potential duplicates (same artist + title)
python music_catalog.py --duplicates

# Export catalog to CSV
python music_catalog.py --export catalog.csv
```

### Reorganizing Your Library

```bash
# Preview changes without modifying files
python music_organizer.py --preview

# Execute the reorganization
python music_organizer.py --execute

# Find duplicates in Mixed folders that exist in complete albums
python music_organizer.py --duplicates

# Remove those duplicates (moves to _Duplicates_Removed folder)
python music_organizer.py --remove-duplicates

# Adjust minimum tracks for "complete" album (default: 5)
python music_organizer.py --preview --min-tracks 8
```

### Finding Partial Albums

Albums with 5+ tracks but non-sequential track numbers likely have missing tracks:

```bash
# Find albums with missing tracks (based on track number gaps)
python music_organizer.py --find-partial

# Rename those folders to "Artist - Album (partial)"
python music_organizer.py --mark-partial
```

## Naming Conventions

### Folders
- Complete albums: `Artist - Album`
- Partial albums (missing tracks): `Artist - Album (partial)`
- Incomplete albums/singles: `Artist - Mixed`

### Files
- Format: `Artist - Title.ext`
- Special characters removed: `< > : " / \ | ? *`

### Artist Name Normalization
The organizer fixes common inconsistencies:
- Case normalization: `grateful dead` → `Grateful Dead`
- Misspellings: `Greatful Dead` → `Grateful Dead`
- "The" prefix handling: `The Moody Blues` kept consistent

## How It Works

### Catalog Database Schema

The `music_catalog.db` SQLite database stores:

- **tracks** - All music files with metadata
  - File info: path, name, size, hash
  - Metadata: artist, album, title, track number, year, genre, duration, bitrate
  - Parsed info: artist/album/title extracted from folder/filename
  - Normalized: lowercase versions for duplicate detection

### Duplicate Detection

Two methods are used:

1. **Exact duplicates** - Files with identical content (same file hash)
2. **Potential duplicates** - Same artist + title (may be different versions)

### Reorganization Logic

1. Albums with fewer than 5 tracks (configurable) are considered "incomplete"
2. Incomplete albums and "Unknown" albums are consolidated into `Artist - Mixed`
3. Complete albums remain in their own folders
4. All files are renamed to `Artist - Title.ext` format

## Workflow for New Music

1. Add new music files to the Music folder
2. Run `python music_catalog.py --scan` to catalog them
3. Run `python music_organizer.py --preview` to see proposed changes
4. Run `python music_organizer.py --execute` to reorganize
5. Run `python music_organizer.py --duplicates` to find duplicates
6. Optionally run `python music_organizer.py --remove-duplicates`

## Safety Features

- **Preview mode** - See all changes before executing
- **Logging** - All file moves are logged with timestamps
- **No permanent deletion** - Removed duplicates go to `_Duplicates_Removed` folder
- **Conflict handling** - Duplicate filenames get `(1)`, `(2)` suffixes
- **Incremental scanning** - Only processes new/modified files on subsequent scans
