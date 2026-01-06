#!/usr/bin/env python3
"""
Music Track Title Lookup

Searches MusicBrainz API to find actual track titles for tracks with
generic names like "Track 1", "Track 2", etc.

Usage:
    python music_lookup.py --find          # Find tracks needing lookup
    python music_lookup.py --lookup        # Look up titles from MusicBrainz
    python music_lookup.py --apply         # Apply the found titles

Requirements:
    pip install requests mutagen
"""

import os
import sys
import re
import sqlite3
import argparse
import time
import json
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

try:
    import requests
except ImportError:
    print("Error: requests library required. Install with: pip install requests")
    sys.exit(1)

try:
    from mutagen import File as MutagenFile
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    print("Warning: mutagen not installed. Cannot update file metadata.")

# Configuration
MUSIC_DIR = Path(__file__).parent
DB_FILE = MUSIC_DIR / "music_catalog.db"
CACHE_FILE = MUSIC_DIR / "musicbrainz_cache.json"

# MusicBrainz API settings
MB_API_URL = "https://musicbrainz.org/ws/2"
MB_USER_AGENT = "MusicLibraryManager/1.0 (https://github.com/CubCouper/music-library-manager)"
MB_RATE_LIMIT = 1.0  # seconds between requests (MusicBrainz requires this)


def safe_print(text):
    """Print text safely, handling Unicode encoding issues."""
    try:
        print(text)
    except UnicodeEncodeError:
        # Replace problematic characters
        print(text.encode('ascii', 'replace').decode('ascii'))


def get_db_connection():
    """Get database connection."""
    if not DB_FILE.exists():
        print("Error: Database not found. Run 'python music_catalog.py --scan' first.")
        sys.exit(1)
    return sqlite3.connect(DB_FILE)


def load_cache():
    """Load cached MusicBrainz results."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {}


def save_cache(cache):
    """Save MusicBrainz results to cache."""
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)


def find_generic_tracks(conn):
    """Find tracks with generic titles like 'Track 1'."""
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, artist, album, title, track_number, file_path
        FROM tracks
        WHERE title LIKE 'Track %'
           OR title LIKE 'Track%'
        ORDER BY artist, album, track_number
    ''')

    tracks = []
    for row in cursor.fetchall():
        track_id, artist, album, title, track_num, file_path = row

        # Extract track number from title if not in metadata
        if track_num is None:
            match = re.search(r'Track\s*(\d+)', title)
            if match:
                track_num = int(match.group(1))

        tracks.append({
            'id': track_id,
            'artist': artist,
            'album': album,
            'title': title,
            'track_number': track_num,
            'file_path': file_path
        })

    return tracks


def search_musicbrainz_album(artist, album):
    """Search MusicBrainz for an album and return track listing."""
    cache = load_cache()
    cache_key = f"{artist}|||{album}"

    if cache_key in cache:
        return cache[cache_key]

    # Clean up artist/album names for search
    artist_clean = re.sub(r'\s*\([^)]*\)', '', artist)  # Remove parentheticals
    album_clean = re.sub(r'\s*\([^)]*\)', '', album)
    album_clean = re.sub(r'\s*\[[^\]]*\]', '', album_clean)  # Remove brackets
    album_clean = album_clean.replace(' - ', ' ').strip()

    # Search for release
    query = f'artist:"{artist_clean}" AND release:"{album_clean}"'
    url = f"{MB_API_URL}/release/?query={quote(query)}&fmt=json&limit=5"

    headers = {'User-Agent': MB_USER_AGENT}

    try:
        time.sleep(MB_RATE_LIMIT)  # Rate limiting
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get('releases'):
            # Try broader search
            query = f'artist:"{artist_clean}" release:"{album_clean.split()[0]}"'
            url = f"{MB_API_URL}/release/?query={quote(query)}&fmt=json&limit=5"
            time.sleep(MB_RATE_LIMIT)
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

        if not data.get('releases'):
            cache[cache_key] = None
            save_cache(cache)
            return None

        # Get the first matching release
        release_id = data['releases'][0]['id']
        release_title = data['releases'][0]['title']

        # Get full release details with recordings
        url = f"{MB_API_URL}/release/{release_id}?inc=recordings&fmt=json"
        time.sleep(MB_RATE_LIMIT)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        release_data = response.json()

        # Extract track listing
        tracks = {}
        for medium in release_data.get('media', []):
            for track in medium.get('tracks', []):
                position = track.get('position', track.get('number'))
                title = track.get('title', '')
                if position and title:
                    tracks[int(position)] = title

        result = {
            'release_id': release_id,
            'release_title': release_title,
            'tracks': tracks
        }

        cache[cache_key] = result
        save_cache(cache)
        return result

    except requests.RequestException as e:
        print(f"  API error: {e}")
        return None


def lookup_tracks(conn):
    """Look up track titles from MusicBrainz."""
    generic_tracks = find_generic_tracks(conn)

    if not generic_tracks:
        print("No tracks with generic titles found.")
        return {}

    # Group by album
    albums = {}
    for track in generic_tracks:
        key = (track['artist'], track['album'])
        if key not in albums:
            albums[key] = []
        albums[key].append(track)

    print(f"\nLooking up {len(albums)} albums on MusicBrainz...\n")

    results = {}
    for (artist, album), tracks in albums.items():
        # Skip unknown/unidentifiable albums
        if 'unknown' in artist.lower() or 'noartist' in artist.lower():
            print(f"Skipping: {artist} - {album} (unidentifiable)")
            continue

        print(f"Searching: {artist} - {album}...")
        mb_data = search_musicbrainz_album(artist, album)

        if mb_data and mb_data.get('tracks'):
            print(f"  Found: {mb_data['release_title']} ({len(mb_data['tracks'])} tracks)")

            matches = []
            for track in tracks:
                track_num = track['track_number']
                # Check both int and string keys (MusicBrainz returns strings)
                track_key = str(track_num) if track_num else None
                if track_key and (track_key in mb_data['tracks'] or track_num in mb_data['tracks']):
                    new_title = mb_data['tracks'].get(track_key) or mb_data['tracks'].get(track_num)
                    matches.append({
                        'track': track,
                        'new_title': new_title
                    })
                    safe_print(f"    Track {track_num}: {track['title']} -> {new_title}")

            if matches:
                results[(artist, album)] = {
                    'mb_data': mb_data,
                    'matches': matches
                }
        else:
            print(f"  Not found on MusicBrainz")

    return results


def apply_titles(conn, results, execute=False):
    """Apply found titles to files and database."""
    if not results:
        print("No titles to apply.")
        return

    cursor = conn.cursor()
    total_matches = sum(len(r['matches']) for r in results.values())

    print(f"\n=== Applying {total_matches} Track Title Updates ===\n")

    updated = 0
    errors = 0

    for (artist, album), data in results.items():
        print(f"\n{artist} - {album}:")

        for match in data['matches']:
            track = match['track']
            new_title = match['new_title']
            old_title = track['title']
            file_path = Path(track['file_path'])

            safe_print(f"  {old_title} -> {new_title}")

            if not execute:
                continue

            try:
                # Update file metadata if mutagen is available
                if MUTAGEN_AVAILABLE and file_path.exists():
                    audio = MutagenFile(file_path, easy=True)
                    if audio is not None and audio.tags is not None:
                        audio.tags['title'] = new_title
                        audio.save()

                # Rename file
                new_filename = f"{artist} - {new_title}{file_path.suffix}"
                # Clean filename
                for char in '<>:"/\\|?*':
                    new_filename = new_filename.replace(char, '')
                new_path = file_path.parent / new_filename

                if file_path.exists() and not new_path.exists():
                    file_path.rename(new_path)

                    # Update database
                    cursor.execute('''
                        UPDATE tracks
                        SET title = ?, file_path = ?, file_name = ?
                        WHERE id = ?
                    ''', (new_title, str(new_path), new_filename, track['id']))

                    updated += 1

            except Exception as e:
                print(f"    Error: {e}")
                errors += 1

    if execute:
        conn.commit()
        print(f"\nUpdated {updated} tracks ({errors} errors)")
    else:
        print(f"\nRun with --apply to update {total_matches} tracks")


def main():
    parser = argparse.ArgumentParser(
        description='Music Track Title Lookup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python music_lookup.py --find      # List tracks needing lookup
  python music_lookup.py --lookup    # Search MusicBrainz for titles
  python music_lookup.py --apply     # Apply found titles to files
        '''
    )
    parser.add_argument('--find', action='store_true',
                       help='Find tracks with generic titles')
    parser.add_argument('--lookup', action='store_true',
                       help='Look up titles from MusicBrainz')
    parser.add_argument('--apply', action='store_true',
                       help='Apply found titles to files and database')
    parser.add_argument('--clear-cache', action='store_true',
                       help='Clear MusicBrainz cache')

    args = parser.parse_args()

    if not any([args.find, args.lookup, args.apply, args.clear_cache]):
        parser.print_help()
        return

    if args.clear_cache:
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
            print("Cache cleared.")
        return

    conn = get_db_connection()

    try:
        if args.find:
            tracks = find_generic_tracks(conn)
            print(f"\nFound {len(tracks)} tracks with generic titles:\n")

            # Group by album
            albums = {}
            for track in tracks:
                key = (track['artist'], track['album'])
                if key not in albums:
                    albums[key] = []
                albums[key].append(track)

            for (artist, album), album_tracks in sorted(albums.items()):
                print(f"{artist} - {album}:")
                for t in sorted(album_tracks, key=lambda x: x['track_number'] or 0):
                    print(f"  Track {t['track_number']}: {t['title']}")
                print()

        elif args.lookup:
            results = lookup_tracks(conn)
            if results:
                apply_titles(conn, results, execute=False)

        elif args.apply:
            results = lookup_tracks(conn)
            if results:
                confirm = input(f"\nApply these title updates? (yes/no): ")
                if confirm.lower() == 'yes':
                    apply_titles(conn, results, execute=True)
                else:
                    print("Cancelled.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
