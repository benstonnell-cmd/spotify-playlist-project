import json
import os
import csv
import time
from collections import defaultdict
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import musicbrainzngs

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

SCOPE = 'playlist-modify-public playlist-modify-private'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    redirect_uri=os.getenv('REDIRECT_URI'),
    scope=SCOPE,
))


def load_streaming_history():
    """Load all audio streaming history JSON files from data/."""
    entries = []
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.startswith('Streaming_History_Audio') and filename.endswith('.json'):
            with open(os.path.join(DATA_DIR, filename), encoding='utf-8') as f:
                entries.extend(json.load(f))
    return entries


def aggregate_tracks(entries, min_ms=30000):
    """Aggregate play counts and listening time per track.

    Args:
        entries: Raw streaming history entries.
        min_ms: Minimum ms_played to count as a real listen (default 30s).

    Returns:
        Dict keyed by track URI with stats.
    """
    tracks = defaultdict(lambda: {
        'track_name': '',
        'artist_name': '',
        'album_name': '',
        'play_count': 0,
        'total_ms': 0,
    })

    for e in entries:
        uri = e.get('spotify_track_uri')
        name = e.get('master_metadata_track_name')
        if not uri or not name:
            continue
        if e['ms_played'] < min_ms:
            continue

        t = tracks[uri]
        t['track_name'] = name
        t['artist_name'] = e.get('master_metadata_album_artist_name', '')
        t['album_name'] = e.get('master_metadata_album_album_name', '')
        t['play_count'] += 1
        t['total_ms'] += e['ms_played']

    return tracks


def aggregate_artists(entries, min_ms=30000):
    """Aggregate play counts and listening time per artist."""
    artists = defaultdict(lambda: {
        'play_count': 0,
        'total_ms': 0,
        'track_uris': set(),
    })

    for e in entries:
        artist = e.get('master_metadata_album_artist_name')
        uri = e.get('spotify_track_uri')
        if not artist or not uri:
            continue
        if e['ms_played'] < min_ms:
            continue

        a = artists[artist]
        a['play_count'] += 1
        a['total_ms'] += e['ms_played']
        a['track_uris'].add(uri)

    return artists


GENRE_CACHE_PATH = os.path.join(DATA_DIR, 'genre_cache.json')


def load_genre_cache():
    if os.path.exists(GENRE_CACHE_PATH):
        with open(GENRE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_genre_cache(cache):
    with open(GENRE_CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_artist_genres(artist_names, max_retries=3):
    """Fetch genres/tags from MusicBrainz (Spotify removed genre data in 2024).

    Caches results to avoid redundant lookups. Retries failures with backoff.

    Returns:
        Dict mapping artist name -> list of genre tags.
    """
    musicbrainzngs.set_useragent('spotify-playlist-project', '1.0')

    cache = load_genre_cache()
    names = list(artist_names)

    # Split into cached hits vs names that need fetching
    to_fetch = [n for n in names if n not in cache]
    print(f'Genre lookup: {len(names)} artists total, {len(names) - len(to_fetch)} cached, {len(to_fetch)} to fetch')

    for attempt in range(1, max_retries + 1):
        if not to_fetch:
            break

        failed = []
        print(f'  Attempt {attempt}: fetching {len(to_fetch)} artists...')

        for i, name in enumerate(to_fetch):
            try:
                result = musicbrainzngs.search_artists(artist=name, limit=1)
                artists = result.get('artist-list', [])
                if artists:
                    tags = artists[0].get('tag-list', [])
                    sorted_tags = sorted(tags, key=lambda t: int(t.get('count', 0)), reverse=True)
                    cache[name] = [t['name'] for t in sorted_tags[:5]]
                else:
                    cache[name] = []
            except Exception as e:
                print(f'    Failed: "{name}": {e}')
                failed.append(name)

            # MusicBrainz rate limit: 1 request per second
            time.sleep(1.1)

            if (i + 1) % 25 == 0:
                print(f'    {i + 1}/{len(to_fetch)} processed')

        to_fetch = failed
        if failed and attempt < max_retries:
            wait = 5 * attempt
            print(f'  {len(failed)} failures, retrying in {wait}s...')
            time.sleep(wait)

    if to_fetch:
        print(f'  Still failed after {max_retries} attempts: {to_fetch}')
        for name in to_fetch:
            cache[name] = []

    save_genre_cache(cache)
    return {name: cache.get(name, []) for name in names}


def ms_to_hours(ms):
    return round(ms / 3_600_000, 1)


def main():
    print('Loading streaming history...')
    entries = load_streaming_history()
    print(f'Loaded {len(entries):,} entries')

    tracks = aggregate_tracks(entries)
    artists = aggregate_artists(entries)

    # Sort by play count
    top_tracks = sorted(tracks.items(), key=lambda x: x[1]['play_count'], reverse=True)
    top_artists = sorted(artists.items(), key=lambda x: x[1]['play_count'], reverse=True)

    # Fetch genres for top 200 artists by name
    top_artist_names = [name for name, _ in top_artists[:200]]
    artist_genres = fetch_artist_genres(top_artist_names)

    # --- Print summaries ---
    print('\n' + '=' * 60)
    print('TOP 50 TRACKS (by play count)')
    print('=' * 60)
    for i, (uri, t) in enumerate(top_tracks[:50], 1):
        hours = ms_to_hours(t['total_ms'])
        genres = artist_genres.get(t['artist_name'], [])
        genre_str = ', '.join(genres[:3]) if genres else 'unknown'
        print(f"{i:3}. {t['track_name']} — {t['artist_name']}")
        print(f"     {t['play_count']} plays | {hours}h | {genre_str}")

    print('\n' + '=' * 60)
    print('TOP 50 ARTISTS (by play count)')
    print('=' * 60)
    for i, (name, a) in enumerate(top_artists[:50], 1):
        hours = ms_to_hours(a['total_ms'])
        genres = artist_genres.get(name, [])
        genre_str = ', '.join(genres[:3]) if genres else 'unknown'
        print(f"{i:3}. {name}")
        print(f"     {a['play_count']} plays | {hours}h | {len(a['track_uris'])} tracks | {genre_str}")

    # --- Genre breakdown ---
    genre_ms = defaultdict(int)
    genre_plays = defaultdict(int)
    for name, a in artists.items():
        genres = artist_genres.get(name, [])
        for genre in genres:
            genre_ms[genre] += a['total_ms']
            genre_plays[genre] += a['play_count']

    top_genres = sorted(genre_ms.items(), key=lambda x: x[1], reverse=True)

    print('\n' + '=' * 60)
    print('TOP 30 GENRES (by listening time)')
    print('=' * 60)
    for i, (genre, ms) in enumerate(top_genres[:30], 1):
        hours = ms_to_hours(ms)
        plays = genre_plays[genre]
        print(f"{i:3}. {genre} — {hours}h | {plays:,} plays")

    # --- Export CSVs ---
    tracks_csv = os.path.join(DATA_DIR, 'top_tracks.csv')
    with open(tracks_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['rank', 'track_name', 'artist_name', 'album_name', 'play_count',
                     'hours_listened', 'genres', 'spotify_uri'])
        for i, (uri, t) in enumerate(top_tracks[:500], 1):
            genres = artist_genres.get(t['artist_name'], [])
            w.writerow([i, t['track_name'], t['artist_name'], t['album_name'],
                        t['play_count'], ms_to_hours(t['total_ms']),
                        '; '.join(genres), uri])

    artists_csv = os.path.join(DATA_DIR, 'top_artists.csv')
    with open(artists_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['rank', 'artist_name', 'play_count', 'hours_listened',
                     'unique_tracks', 'genres'])
        for i, (name, a) in enumerate(top_artists[:200], 1):
            genres = artist_genres.get(name, [])
            w.writerow([i, name, a['play_count'], ms_to_hours(a['total_ms']),
                        len(a['track_uris']), '; '.join(genres)])

    genres_csv = os.path.join(DATA_DIR, 'top_genres.csv')
    with open(genres_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['rank', 'genre', 'hours_listened', 'play_count'])
        for i, (genre, ms) in enumerate(top_genres, 1):
            w.writerow([i, genre, ms_to_hours(ms), genre_plays[genre]])

    print(f'\nExported:')
    print(f'  {tracks_csv} (top 500 tracks)')
    print(f'  {artists_csv} (top 200 artists)')
    print(f'  {genres_csv} (all genres)')


if __name__ == '__main__':
    main()
