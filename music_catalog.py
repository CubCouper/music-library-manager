#!/usr/bin/env python3
"""
Music Library Catalog Tool

Scans a music folder, extracts metadata, stores in SQLite database,
and identifies duplicate tracks. Can be run incrementally.

Usage:
    python music_catalog.py [OPTIONS]

Options:
    --scan          Scan library and update database
    --duplicates    Show potential duplicate tracks
    --stats         Show library statistics
    --export CSV    Export catalog to CSV file
    --help          Show this help message

Requirements:
    pip install mutagen
"""

import os
import sys
import sqlite3
import hashlib
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Try to import mutagen for metadata extraction
try:
    from mutagen import File as MutagenFile
    from mutagen.mp3 import MP3
    from mutagen.mp4 import MP4
    from mutagen.flac import FLAC
    from mutagen.id3 import ID3
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    print("Warning: mutagen not installed. Install with: pip install mutagen")
    print("Falling back to filename parsing only.\n")

# Configuration
MUSIC_DIR = Path(__file__).parent
DB_FILE = MUSIC_DIR / "music_catalog.db"
AUDIO_EXTENSIONS = {'.mp3', '.m4a', '.wav', '.flac', '.ogg', '.wma', '.aac'}


def safe_print(text):
    """Print text safely, handling Unicode encoding issues on Windows."""
    try:
        print(text)
    except UnicodeEncodeError:
        # Replace problematic characters
        print(text.encode('ascii', 'replace').decode('ascii'))


def create_database():
    """Create the SQLite database schema."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Main tracks table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER,
            file_hash TEXT,

            -- Metadata from tags
            artist TEXT,
            album TEXT,
            title TEXT,
            track_number INTEGER,
            year INTEGER,
            genre TEXT,
            duration_seconds REAL,
            bitrate INTEGER,

            -- Parsed from folder/filename (fallback)
            parsed_artist TEXT,
            parsed_album TEXT,
            parsed_title TEXT,

            -- Normalized versions for duplicate detection
            artist_normalized TEXT,
            title_normalized TEXT,

            -- Timestamps
            date_added TEXT,
            date_modified TEXT,
            last_scanned TEXT
        )
    ''')

    # Index for faster duplicate detection
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_artist_title
        ON tracks(artist_normalized, title_normalized)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_file_hash
        ON tracks(file_hash)
    ''')

    # Duplicate groups table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS duplicate_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_hash TEXT UNIQUE,
            track_count INTEGER,
            date_identified TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS duplicate_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            track_id INTEGER,
            FOREIGN KEY (group_id) REFERENCES duplicate_groups(id),
            FOREIGN KEY (track_id) REFERENCES tracks(id)
        )
    ''')

    conn.commit()
    return conn


def normalize_string(s):
    """Normalize a string for comparison (lowercase, remove special chars)."""
    if not s:
        return ""
    import re
    # Convert to lowercase
    s = s.lower()
    # Remove common prefixes like "the "
    s = re.sub(r'^the\s+', '', s)
    # Remove special characters and extra whitespace
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def parse_folder_name(folder_name):
    """Parse 'Artist - Album' format from folder name."""
    if ' - ' in folder_name:
        parts = folder_name.split(' - ', 1)
        return parts[0].strip(), parts[1].strip()
    return folder_name.strip(), "Unknown"


def parse_file_name(file_name):
    """Parse 'Artist - Title' format from filename."""
    name = Path(file_name).stem
    if ' - ' in name:
        parts = name.split(' - ', 1)
        return parts[0].strip(), parts[1].strip()
    return "Unknown", name.strip()


def get_file_hash(file_path, quick=True):
    """
    Calculate hash of file for duplicate detection.
    If quick=True, only hash first and last 64KB (faster for large files).
    """
    try:
        file_size = os.path.getsize(file_path)
        hasher = hashlib.md5()

        with open(file_path, 'rb') as f:
            if quick and file_size > 131072:  # 128KB
                # Hash first 64KB
                hasher.update(f.read(65536))
                # Hash last 64KB
                f.seek(-65536, 2)
                hasher.update(f.read(65536))
                # Include file size in hash
                hasher.update(str(file_size).encode())
            else:
                # Hash entire file for small files
                for chunk in iter(lambda: f.read(65536), b''):
                    hasher.update(chunk)

        return hasher.hexdigest()
    except Exception as e:
        safe_print(f"  Error hashing {file_path}: {e}")
        return None


def extract_metadata(file_path):
    """Extract metadata from audio file using mutagen."""
    metadata = {
        'artist': None,
        'album': None,
        'title': None,
        'track_number': None,
        'year': None,
        'genre': None,
        'duration_seconds': None,
        'bitrate': None
    }

    if not MUTAGEN_AVAILABLE:
        return metadata

    try:
        audio = MutagenFile(file_path, easy=True)
        if audio is None:
            return metadata

        # Duration
        if hasattr(audio.info, 'length'):
            metadata['duration_seconds'] = audio.info.length

        # Bitrate
        if hasattr(audio.info, 'bitrate'):
            metadata['bitrate'] = audio.info.bitrate

        # Try to get tags
        if audio.tags:
            # Common tag names vary by format
            tag_mappings = {
                'artist': ['artist', 'TPE1', '\xa9ART', 'ARTIST'],
                'album': ['album', 'TALB', '\xa9alb', 'ALBUM'],
                'title': ['title', 'TIT2', '\xa9nam', 'TITLE'],
                'genre': ['genre', 'TCON', '\xa9gen', 'GENRE'],
                'date': ['date', 'TDRC', '\xa9day', 'DATE', 'year', 'TYER'],
                'tracknumber': ['tracknumber', 'TRCK', 'trkn', 'TRACKNUMBER']
            }

            for field, possible_tags in tag_mappings.items():
                for tag in possible_tags:
                    try:
                        value = audio.tags.get(tag)
                        if value:
                            if isinstance(value, list):
                                value = value[0]
                            value = str(value).strip()
                            if value:
                                if field == 'date':
                                    # Extract year from date
                                    try:
                                        metadata['year'] = int(value[:4])
                                    except:
                                        pass
                                elif field == 'tracknumber':
                                    # Handle "1/12" format
                                    try:
                                        metadata['track_number'] = int(str(value).split('/')[0])
                                    except:
                                        pass
                                else:
                                    metadata[field] = value
                                break
                    except:
                        continue

    except Exception as e:
        # Silently fail - we'll use filename parsing
        pass

    return metadata


def scan_library(conn, force_rescan=False):
    """Scan music library and update database."""
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # Get existing files in database
    cursor.execute("SELECT file_path, last_scanned FROM tracks")
    existing = {row[0]: row[1] for row in cursor.fetchall()}

    # Track statistics
    stats = {'new': 0, 'updated': 0, 'skipped': 0, 'removed': 0, 'errors': 0}
    found_files = set()

    print(f"Scanning {MUSIC_DIR}...")

    # Walk through music directory
    for root, dirs, files in os.walk(MUSIC_DIR):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            ext = Path(filename).suffix.lower()
            if ext not in AUDIO_EXTENSIONS:
                continue

            file_path = Path(root) / filename
            file_path_str = str(file_path)
            found_files.add(file_path_str)

            # Check if file needs processing
            if file_path_str in existing and not force_rescan:
                try:
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                    if existing[file_path_str] >= file_mtime:
                        stats['skipped'] += 1
                        continue
                except:
                    pass

            try:
                # Get file info
                file_size = os.path.getsize(file_path)
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()

                # Parse folder and filename
                folder_name = Path(root).name
                parsed_artist, parsed_album = parse_folder_name(folder_name)
                _, parsed_title = parse_file_name(filename)

                # Extract metadata from tags
                metadata = extract_metadata(file_path)

                # Use metadata if available, otherwise use parsed values
                artist = metadata['artist'] or parsed_artist
                album = metadata['album'] or parsed_album
                title = metadata['title'] or parsed_title

                # Calculate file hash for duplicate detection
                file_hash = get_file_hash(file_path)

                # Normalize for duplicate detection
                artist_normalized = normalize_string(artist)
                title_normalized = normalize_string(title)

                # Insert or update
                if file_path_str in existing:
                    cursor.execute('''
                        UPDATE tracks SET
                            file_name = ?, file_size = ?, file_hash = ?,
                            artist = ?, album = ?, title = ?,
                            track_number = ?, year = ?, genre = ?,
                            duration_seconds = ?, bitrate = ?,
                            parsed_artist = ?, parsed_album = ?, parsed_title = ?,
                            artist_normalized = ?, title_normalized = ?,
                            date_modified = ?, last_scanned = ?
                        WHERE file_path = ?
                    ''', (
                        filename, file_size, file_hash,
                        artist, album, title,
                        metadata['track_number'], metadata['year'], metadata['genre'],
                        metadata['duration_seconds'], metadata['bitrate'],
                        parsed_artist, parsed_album, parsed_title,
                        artist_normalized, title_normalized,
                        file_mtime, now,
                        file_path_str
                    ))
                    stats['updated'] += 1
                    safe_print(f"  Updated: {artist} - {title}")
                else:
                    cursor.execute('''
                        INSERT INTO tracks (
                            file_path, file_name, file_size, file_hash,
                            artist, album, title,
                            track_number, year, genre,
                            duration_seconds, bitrate,
                            parsed_artist, parsed_album, parsed_title,
                            artist_normalized, title_normalized,
                            date_added, date_modified, last_scanned
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        file_path_str, filename, file_size, file_hash,
                        artist, album, title,
                        metadata['track_number'], metadata['year'], metadata['genre'],
                        metadata['duration_seconds'], metadata['bitrate'],
                        parsed_artist, parsed_album, parsed_title,
                        artist_normalized, title_normalized,
                        now, file_mtime, now
                    ))
                    stats['new'] += 1
                    safe_print(f"  Added: {artist} - {title}")

            except Exception as e:
                safe_print(f"  Error processing {file_path}: {e}")
                stats['errors'] += 1

    # Remove entries for deleted files
    for file_path in existing:
        if file_path not in found_files:
            cursor.execute("DELETE FROM tracks WHERE file_path = ?", (file_path,))
            stats['removed'] += 1
            safe_print(f"  Removed: {file_path}")

    conn.commit()

    print(f"\nScan complete:")
    print(f"  New files: {stats['new']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped (unchanged): {stats['skipped']}")
    print(f"  Removed (deleted): {stats['removed']}")
    print(f"  Errors: {stats['errors']}")

    return stats


def find_duplicates(conn):
    """Find potential duplicate tracks."""
    cursor = conn.cursor()

    duplicates = []

    # Method 1: Same file hash (exact duplicates)
    print("\n=== Exact Duplicates (same file content) ===")
    cursor.execute('''
        SELECT file_hash, COUNT(*) as cnt
        FROM tracks
        WHERE file_hash IS NOT NULL
        GROUP BY file_hash
        HAVING cnt > 1
        ORDER BY cnt DESC
    ''')

    hash_duplicates = cursor.fetchall()
    for file_hash, count in hash_duplicates:
        cursor.execute('''
            SELECT artist, title, album, file_path, file_size
            FROM tracks WHERE file_hash = ?
        ''', (file_hash,))
        tracks = cursor.fetchall()
        print(f"\nDuplicate group ({count} files with identical content):")
        for artist, title, album, path, size in tracks:
            safe_print(f"  [{size//1024}KB] {artist} - {title}")
            safe_print(f"           Album: {album}")
            safe_print(f"           Path: {path}")
        duplicates.append(('exact', tracks))

    # Method 2: Same normalized artist + title (potential duplicates)
    print("\n=== Potential Duplicates (same artist + title) ===")
    cursor.execute('''
        SELECT artist_normalized, title_normalized, COUNT(*) as cnt
        FROM tracks
        WHERE artist_normalized != '' AND title_normalized != ''
        GROUP BY artist_normalized, title_normalized
        HAVING cnt > 1
        ORDER BY cnt DESC
    ''')

    title_duplicates = cursor.fetchall()
    for artist_norm, title_norm, count in title_duplicates:
        # Skip if already found as exact duplicate
        cursor.execute('''
            SELECT artist, title, album, file_path, file_size, duration_seconds, bitrate
            FROM tracks
            WHERE artist_normalized = ? AND title_normalized = ?
        ''', (artist_norm, title_norm))
        tracks = cursor.fetchall()

        # Check if all have same hash (already reported)
        cursor.execute('''
            SELECT COUNT(DISTINCT file_hash)
            FROM tracks
            WHERE artist_normalized = ? AND title_normalized = ?
            AND file_hash IS NOT NULL
        ''', (artist_norm, title_norm))
        distinct_hashes = cursor.fetchone()[0]

        if distinct_hashes > 1 or distinct_hashes == 0:
            safe_print(f"\nPotential duplicate: '{artist_norm}' - '{title_norm}' ({count} versions):")
            for artist, title, album, path, size, duration, bitrate in tracks:
                duration_str = f"{int(duration//60)}:{int(duration%60):02d}" if duration else "?"
                bitrate_str = f"{bitrate//1000}kbps" if bitrate else "?"
                safe_print(f"  [{duration_str}, {bitrate_str}] {artist} - {title}")
                safe_print(f"           Album: {album}")
                safe_print(f"           Path: {path}")
            duplicates.append(('potential', tracks))

    if not hash_duplicates and not title_duplicates:
        print("No duplicates found!")

    return duplicates


def show_statistics(conn):
    """Show library statistics."""
    cursor = conn.cursor()

    print("\n=== Music Library Statistics ===\n")

    # Total tracks
    cursor.execute("SELECT COUNT(*) FROM tracks")
    total = cursor.fetchone()[0]
    print(f"Total tracks: {total}")

    # Total size
    cursor.execute("SELECT SUM(file_size) FROM tracks")
    total_size = cursor.fetchone()[0] or 0
    print(f"Total size: {total_size / (1024**3):.2f} GB")

    # Total duration
    cursor.execute("SELECT SUM(duration_seconds) FROM tracks WHERE duration_seconds IS NOT NULL")
    total_duration = cursor.fetchone()[0] or 0
    hours = total_duration // 3600
    minutes = (total_duration % 3600) // 60
    print(f"Total duration: {int(hours)} hours, {int(minutes)} minutes")

    # By file format
    print("\nBy file format:")
    cursor.execute("SELECT file_name FROM tracks")
    ext_counts = defaultdict(int)
    for (file_name,) in cursor.fetchall():
        ext = Path(file_name).suffix.lower()
        ext_counts[ext] += 1
    for ext, count in sorted(ext_counts.items(), key=lambda x: -x[1]):
        print(f"  {ext}: {count} files")

    # Unique artists
    cursor.execute("SELECT COUNT(DISTINCT artist_normalized) FROM tracks WHERE artist_normalized != ''")
    print(f"\nUnique artists: {cursor.fetchone()[0]}")

    # Top artists
    print("\nTop 10 artists by track count:")
    cursor.execute('''
        SELECT artist, COUNT(*) as cnt
        FROM tracks
        WHERE artist IS NOT NULL AND artist != ''
        GROUP BY artist_normalized
        ORDER BY cnt DESC
        LIMIT 10
    ''')
    for artist, count in cursor.fetchall():
        safe_print(f"  {artist}: {count} tracks")

    # Unique albums
    cursor.execute("SELECT COUNT(DISTINCT album) FROM tracks WHERE album IS NOT NULL AND album != ''")
    print(f"\nUnique albums: {cursor.fetchone()[0]}")

    # Tracks with missing metadata
    cursor.execute('''
        SELECT COUNT(*) FROM tracks
        WHERE artist IS NULL OR artist = ''
           OR title IS NULL OR title = ''
    ''')
    missing = cursor.fetchone()[0]
    if missing > 0:
        print(f"\nTracks with missing metadata: {missing}")

    # Potential duplicates count
    cursor.execute('''
        SELECT COUNT(*) FROM (
            SELECT artist_normalized, title_normalized
            FROM tracks
            WHERE artist_normalized != '' AND title_normalized != ''
            GROUP BY artist_normalized, title_normalized
            HAVING COUNT(*) > 1
        )
    ''')
    dup_groups = cursor.fetchone()[0]
    if dup_groups > 0:
        print(f"Potential duplicate groups: {dup_groups}")


def export_to_csv(conn, output_file):
    """Export catalog to CSV file."""
    import csv

    cursor = conn.cursor()
    cursor.execute('''
        SELECT artist, album, title, year, genre,
               duration_seconds, bitrate, file_path
        FROM tracks
        ORDER BY artist, album, track_number, title
    ''')

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Artist', 'Album', 'Title', 'Year', 'Genre',
                        'Duration', 'Bitrate', 'File Path'])

        for row in cursor.fetchall():
            artist, album, title, year, genre, duration, bitrate, path = row
            duration_str = f"{int(duration//60)}:{int(duration%60):02d}" if duration else ""
            bitrate_str = f"{bitrate//1000}kbps" if bitrate else ""
            writer.writerow([artist, album, title, year or '', genre or '',
                           duration_str, bitrate_str, path])

    print(f"Exported to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Music Library Catalog Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python music_catalog.py --scan          # Scan library and update database
  python music_catalog.py --duplicates    # Find duplicate tracks
  python music_catalog.py --stats         # Show library statistics
  python music_catalog.py --export catalog.csv  # Export to CSV
        '''
    )
    parser.add_argument('--scan', action='store_true',
                       help='Scan library and update database')
    parser.add_argument('--rescan', action='store_true',
                       help='Force rescan all files (ignore cached data)')
    parser.add_argument('--duplicates', action='store_true',
                       help='Find potential duplicate tracks')
    parser.add_argument('--stats', action='store_true',
                       help='Show library statistics')
    parser.add_argument('--export', metavar='FILE',
                       help='Export catalog to CSV file')

    args = parser.parse_args()

    # Default to showing help if no arguments
    if not any([args.scan, args.rescan, args.duplicates, args.stats, args.export]):
        parser.print_help()
        return

    # Create/open database
    conn = create_database()

    try:
        if args.scan or args.rescan:
            scan_library(conn, force_rescan=args.rescan)

        if args.duplicates:
            find_duplicates(conn)

        if args.stats:
            show_statistics(conn)

        if args.export:
            export_to_csv(conn, args.export)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
