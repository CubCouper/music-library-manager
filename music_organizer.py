#!/usr/bin/env python3
"""
Music Library Organizer

Standardizes folder and file naming, consolidates incomplete albums into
"Artist - Mixed" folders, and identifies duplicates for removal.

Usage:
    python music_organizer.py --preview     # Preview changes (no modifications)
    python music_organizer.py --execute     # Execute the reorganization
    python music_organizer.py --duplicates  # Find duplicates in Mixed folders

Requirements:
    pip install mutagen
"""

import os
import sys
import re
import sqlite3
import shutil
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Configuration
MUSIC_DIR = Path(__file__).parent
DB_FILE = MUSIC_DIR / "music_catalog.db"

# Minimum tracks to consider an album "complete" (albums with fewer tracks
# will be consolidated into "Artist - Mixed")
MIN_ALBUM_TRACKS = 5

# Artist name corrections (normalized -> preferred spelling)
ARTIST_CORRECTIONS = {
    # Fix misspellings
    "greatful dead": "Grateful Dead",
    # Normalize "The" prefix
    "grateful dead": "Grateful Dead",
    "moody blues": "The Moody Blues",
    "velvet underground": "The Velvet Underground",
    # Fix case issues
    "marty robins": "Marty Robbins",
}

# Characters to clean from filenames (Windows-safe)
UNSAFE_CHARS = r'<>:"/\|?*'


def get_db_connection():
    """Get database connection."""
    if not DB_FILE.exists():
        print("Error: Database not found. Run 'python music_catalog.py --scan' first.")
        sys.exit(1)
    return sqlite3.connect(DB_FILE)


def clean_filename(name):
    """Clean a string for use as a filename."""
    if not name:
        return "Unknown"

    # Remove or replace unsafe characters
    for char in UNSAFE_CHARS:
        name = name.replace(char, '')

    # Replace multiple spaces with single space
    name = re.sub(r'\s+', ' ', name)

    # Remove leading/trailing whitespace and dots
    name = name.strip(' .')

    # Limit length (Windows MAX_PATH considerations)
    if len(name) > 100:
        name = name[:100].rsplit(' ', 1)[0]

    return name or "Unknown"


def normalize_artist(artist):
    """Get the normalized form of an artist name for lookups."""
    if not artist:
        return ""
    s = artist.lower()
    s = re.sub(r'^the\s+', '', s)
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def get_preferred_artist_name(artist, all_variants):
    """
    Determine the preferred artist name from variants.
    Uses corrections dict, or picks the most common/properly cased variant.
    """
    normalized = normalize_artist(artist)

    # Check corrections dictionary
    if normalized in ARTIST_CORRECTIONS:
        return ARTIST_CORRECTIONS[normalized]

    # If we have variants, pick the best one
    if normalized in all_variants:
        variants = all_variants[normalized]
        # Prefer title case, most tracks
        best = max(variants, key=lambda x: (
            x[0][0].isupper(),  # Starts with capital
            x[0] == x[0].title() or x[0][0:4] == "The ",  # Title case or starts with "The"
            x[1]  # Track count
        ))
        return best[0]

    return artist


def analyze_library(conn, min_tracks=5):
    """Analyze the library and return reorganization plan."""
    cursor = conn.cursor()

    # Get all artist name variants
    cursor.execute('''
        SELECT artist, artist_normalized, COUNT(*) as cnt
        FROM tracks
        GROUP BY artist
    ''')
    artist_variants = defaultdict(list)
    for artist, norm, cnt in cursor.fetchall():
        artist_variants[norm].append((artist, cnt))

    # Get album info
    cursor.execute('''
        SELECT artist, album, COUNT(*) as track_count,
               GROUP_CONCAT(id) as track_ids
        FROM tracks
        GROUP BY artist, album
        ORDER BY artist, album
    ''')

    albums = []
    for artist, album, track_count, track_ids in cursor.fetchall():
        preferred_artist = get_preferred_artist_name(artist, artist_variants)
        is_complete = track_count >= min_tracks
        is_unknown = album and ('unknown' in album.lower() or album.strip() == '')

        albums.append({
            'original_artist': artist,
            'preferred_artist': preferred_artist,
            'album': album,
            'track_count': track_count,
            'track_ids': [int(x) for x in track_ids.split(',')],
            'is_complete': is_complete and not is_unknown,
            'consolidate_to_mixed': not is_complete or is_unknown
        })

    return albums, artist_variants


def generate_moves(conn, albums):
    """Generate list of file moves based on album analysis."""
    cursor = conn.cursor()
    moves = []

    for album_info in albums:
        artist = album_info['preferred_artist']
        album = album_info['album']

        # Determine target folder
        if album_info['consolidate_to_mixed']:
            target_folder = f"{clean_filename(artist)} - Mixed"
        else:
            target_folder = f"{clean_filename(artist)} - {clean_filename(album)}"

        # Get tracks in this album
        placeholders = ','.join('?' * len(album_info['track_ids']))
        cursor.execute(f'''
            SELECT id, file_path, file_name, artist, title
            FROM tracks
            WHERE id IN ({placeholders})
        ''', album_info['track_ids'])

        for track_id, file_path, file_name, track_artist, title in cursor.fetchall():
            # Generate new filename: "Artist - Title.ext"
            ext = Path(file_name).suffix
            new_filename = f"{clean_filename(artist)} - {clean_filename(title)}{ext}"

            source = Path(file_path)
            target_dir = MUSIC_DIR / target_folder
            target = target_dir / new_filename

            # Check if move is needed
            if source != target:
                moves.append({
                    'track_id': track_id,
                    'source': source,
                    'target': target,
                    'target_dir': target_dir,
                    'artist': artist,
                    'title': title,
                    'from_album': album,
                    'to_mixed': album_info['consolidate_to_mixed']
                })

    return moves


def find_duplicates_in_mixed(conn, min_tracks=5):
    """Find tracks in Mixed folders that also exist in complete albums."""
    cursor = conn.cursor()

    print("\n=== Duplicates in Mixed Folders ===")
    print("(These tracks exist both in a Mixed folder and a complete album)\n")

    # Get all tracks, identify which are in "Mixed" folders
    cursor.execute('''
        SELECT id, file_path, artist, title, album,
               artist_normalized, title_normalized
        FROM tracks
    ''')

    all_tracks = cursor.fetchall()

    # Separate into mixed vs complete album tracks
    mixed_tracks = []
    album_tracks = defaultdict(list)  # (artist_norm, title_norm) -> [tracks]

    for track in all_tracks:
        track_id, file_path, artist, title, album, artist_norm, title_norm = track

        # Determine if in a "complete" album (based on current folder, before reorg)
        cursor.execute('''
            SELECT COUNT(*) FROM tracks
            WHERE artist = ? AND album = ?
        ''', (artist, album))
        album_track_count = cursor.fetchone()[0]

        is_in_mixed = (' - Mixed' in file_path or
                       'Unknown' in (album or '') or
                       album_track_count < min_tracks)

        key = (artist_norm, title_norm)

        if is_in_mixed:
            mixed_tracks.append({
                'id': track_id,
                'path': file_path,
                'artist': artist,
                'title': title,
                'album': album,
                'key': key
            })
        else:
            album_tracks[key].append({
                'id': track_id,
                'path': file_path,
                'artist': artist,
                'title': title,
                'album': album
            })

    # Find duplicates
    duplicates = []
    for mixed in mixed_tracks:
        if mixed['key'] in album_tracks and mixed['key'][0] and mixed['key'][1]:
            album_copies = album_tracks[mixed['key']]
            duplicates.append({
                'mixed_track': mixed,
                'album_copies': album_copies
            })

    if not duplicates:
        print("No duplicates found between Mixed folders and complete albums.")
        return []

    print(f"Found {len(duplicates)} tracks in Mixed folders that exist in complete albums:\n")

    for i, dup in enumerate(duplicates, 1):
        mixed = dup['mixed_track']
        print(f"{i}. {mixed['artist']} - {mixed['title']}")
        print(f"   REMOVE (Mixed): {mixed['path']}")
        for album_copy in dup['album_copies']:
            print(f"   KEEP (Album):   {album_copy['album']}")
            print(f"                   {album_copy['path']}")
        print()

    return duplicates


def preview_changes(moves, albums):
    """Show preview of changes without executing."""
    print("\n" + "="*60)
    print("REORGANIZATION PREVIEW")
    print("="*60)

    # Summary by action type
    consolidations = [m for m in moves if m['to_mixed']]
    renames = [m for m in moves if not m['to_mixed']]

    print(f"\nTotal files to process: {len(moves)}")
    print(f"  - Consolidate to Mixed folders: {len(consolidations)}")
    print(f"  - Rename/reorganize in albums: {len(renames)}")

    # Show consolidations grouped by artist
    if consolidations:
        print("\n--- Files to consolidate into Mixed folders ---")
        by_artist = defaultdict(list)
        for m in consolidations:
            by_artist[m['artist']].append(m)

        for artist in sorted(by_artist.keys()):
            artist_moves = by_artist[artist]
            print(f"\n{artist} - Mixed ({len(artist_moves)} tracks):")
            for m in artist_moves[:5]:
                print(f"  <- {m['from_album']}: {m['title']}")
            if len(artist_moves) > 5:
                print(f"  ... and {len(artist_moves) - 5} more")

    # Show sample renames
    if renames:
        print("\n--- Sample file renames (first 10) ---")
        for m in renames[:10]:
            src_name = m['source'].name
            tgt_name = m['target'].name
            if src_name != tgt_name:
                print(f"  {src_name}")
                print(f"    -> {tgt_name}")

    # Folders that will be created
    new_folders = set(m['target_dir'] for m in moves if not m['target_dir'].exists())
    if new_folders:
        print(f"\n--- New folders to create ({len(new_folders)}) ---")
        for folder in sorted(new_folders)[:10]:
            print(f"  {folder.name}")
        if len(new_folders) > 10:
            print(f"  ... and {len(new_folders) - 10} more")

    # Folders that will become empty (candidates for deletion)
    source_folders = set(m['source'].parent for m in moves)
    print(f"\n--- Folders that may become empty ({len(source_folders)}) ---")
    for folder in sorted(source_folders)[:10]:
        print(f"  {folder.name}")
    if len(source_folders) > 10:
        print(f"  ... and {len(source_folders) - 10} more")

    print("\n" + "="*60)
    print("Run with --execute to apply these changes")
    print("="*60)


def execute_moves(conn, moves):
    """Execute the file moves and update database."""
    cursor = conn.cursor()

    print("\n" + "="*60)
    print("EXECUTING REORGANIZATION")
    print("="*60)

    success = 0
    errors = 0
    skipped = 0

    # Create backup log
    log_file = MUSIC_DIR / f"reorganize_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    with open(log_file, 'w', encoding='utf-8') as log:
        log.write("Music Library Reorganization Log\n")
        log.write(f"Date: {datetime.now().isoformat()}\n")
        log.write("="*60 + "\n\n")

        for i, move in enumerate(moves, 1):
            source = move['source']
            target = move['target']

            try:
                # Skip if source doesn't exist
                if not source.exists():
                    log.write(f"SKIP (missing): {source}\n")
                    skipped += 1
                    continue

                # Skip if target already exists with same name
                if source == target:
                    skipped += 1
                    continue

                # Handle target already exists
                if target.exists():
                    # Add number suffix
                    stem = target.stem
                    suffix = target.suffix
                    counter = 1
                    while target.exists():
                        target = target.parent / f"{stem} ({counter}){suffix}"
                        counter += 1

                # Create target directory
                target.parent.mkdir(parents=True, exist_ok=True)

                # Move file
                shutil.move(str(source), str(target))

                # Update database
                cursor.execute('''
                    UPDATE tracks
                    SET file_path = ?, file_name = ?,
                        artist = ?, album = ?
                    WHERE id = ?
                ''', (
                    str(target),
                    target.name,
                    move['artist'],
                    "Mixed" if move['to_mixed'] else move['from_album'],
                    move['track_id']
                ))

                log.write(f"MOVED: {source}\n")
                log.write(f"    -> {target}\n")

                success += 1

                if i % 50 == 0:
                    print(f"  Processed {i}/{len(moves)} files...")
                    conn.commit()

            except Exception as e:
                log.write(f"ERROR: {source}\n")
                log.write(f"       {str(e)}\n")
                errors += 1

        conn.commit()

        log.write(f"\n{'='*60}\n")
        log.write(f"Summary: {success} moved, {skipped} skipped, {errors} errors\n")

    print(f"\nComplete!")
    print(f"  Moved: {success}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")
    print(f"\nLog saved to: {log_file}")

    # Clean up empty directories
    print("\nCleaning up empty directories...")
    cleanup_empty_dirs()

    return success, errors


def cleanup_empty_dirs():
    """Remove empty directories after reorganization."""
    removed = 0
    for root, dirs, files in os.walk(MUSIC_DIR, topdown=False):
        for d in dirs:
            dir_path = Path(root) / d
            try:
                if dir_path.is_dir() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    print(f"  Removed empty: {dir_path.name}")
                    removed += 1
            except Exception:
                pass
    print(f"  Removed {removed} empty directories")


def remove_duplicates(conn, duplicates, execute=False):
    """Remove duplicate tracks from Mixed folders."""
    if not duplicates:
        print("No duplicates to remove.")
        return

    cursor = conn.cursor()

    if not execute:
        print("\nTo remove these duplicates, run:")
        print("  python music_organizer.py --remove-duplicates")
        return

    print("\n=== Removing Duplicates from Mixed Folders ===\n")

    removed = 0
    errors = 0

    for dup in duplicates:
        mixed = dup['mixed_track']
        path = Path(mixed['path'])

        try:
            if path.exists():
                # Move to trash folder instead of deleting
                trash_dir = MUSIC_DIR / "_Duplicates_Removed"
                trash_dir.mkdir(exist_ok=True)

                trash_path = trash_dir / path.name
                counter = 1
                while trash_path.exists():
                    trash_path = trash_dir / f"{path.stem} ({counter}){path.suffix}"
                    counter += 1

                shutil.move(str(path), str(trash_path))

                # Remove from database
                cursor.execute("DELETE FROM tracks WHERE id = ?", (mixed['id'],))

                print(f"  Removed: {mixed['artist']} - {mixed['title']}")
                removed += 1
        except Exception as e:
            print(f"  Error removing {path}: {e}")
            errors += 1

    conn.commit()
    cleanup_empty_dirs()

    print(f"\nRemoved {removed} duplicates ({errors} errors)")
    print(f"Files moved to: {MUSIC_DIR / '_Duplicates_Removed'}")


def main():
    parser = argparse.ArgumentParser(
        description='Music Library Organizer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python music_organizer.py --preview          # Preview reorganization
  python music_organizer.py --execute          # Execute reorganization
  python music_organizer.py --duplicates       # Find duplicates in Mixed
  python music_organizer.py --remove-duplicates # Remove duplicates
        '''
    )
    parser.add_argument('--preview', action='store_true',
                       help='Preview changes without executing')
    parser.add_argument('--execute', action='store_true',
                       help='Execute the reorganization')
    parser.add_argument('--duplicates', action='store_true',
                       help='Find duplicates in Mixed folders')
    parser.add_argument('--remove-duplicates', action='store_true',
                       help='Remove duplicates from Mixed folders')
    parser.add_argument('--min-tracks', type=int, default=5,
                       help='Minimum tracks for complete album (default: 5)')

    args = parser.parse_args()

    if not any([args.preview, args.execute, args.duplicates, args.remove_duplicates]):
        parser.print_help()
        return

    min_tracks = args.min_tracks

    conn = get_db_connection()

    try:
        if args.duplicates:
            find_duplicates_in_mixed(conn, min_tracks)

        elif args.remove_duplicates:
            duplicates = find_duplicates_in_mixed(conn, min_tracks)
            if duplicates:
                confirm = input("\nRemove these duplicates? (yes/no): ")
                if confirm.lower() == 'yes':
                    remove_duplicates(conn, duplicates, execute=True)
                else:
                    print("Cancelled.")

        elif args.preview or args.execute:
            print("Analyzing library...")
            albums, artist_variants = analyze_library(conn, min_tracks)

            print("Generating move plan...")
            moves = generate_moves(conn, albums)

            if args.preview:
                preview_changes(moves, albums)
            else:
                confirm = input(f"\nThis will reorganize {len(moves)} files. Continue? (yes/no): ")
                if confirm.lower() == 'yes':
                    execute_moves(conn, moves)
                    print("\nRun 'python music_catalog.py --scan' to update the catalog.")
                else:
                    print("Cancelled.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
