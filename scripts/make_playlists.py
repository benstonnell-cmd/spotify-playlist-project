import json
import os
from collections import defaultdict
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
GENRE_CACHE_PATH = os.path.join(DATA_DIR, 'genre_cache.json')

SCOPE = 'playlist-modify-public playlist-modify-private'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    redirect_uri=os.getenv('REDIRECT_URI'),
    scope=SCOPE,
))


def load_streaming_history():
    entries = []
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.startswith('Streaming_History_Audio') and filename.endswith('.json'):
            with open(os.path.join(DATA_DIR, filename), encoding='utf-8') as f:
                entries.extend(json.load(f))
    return entries


def load_genre_cache():
    if os.path.exists(GENRE_CACHE_PATH):
        with open(GENRE_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    return {}


def aggregate_tracks(entries, min_ms=30000):
    tracks = defaultdict(lambda: {
        'track_name': '',
        'artist_name': '',
        'play_count': 0,
        'total_ms': 0,
    })
    for e in entries:
        uri = e.get('spotify_track_uri')
        name = e.get('master_metadata_track_name')
        if not uri or not name or e['ms_played'] < min_ms:
            continue
        t = tracks[uri]
        t['track_name'] = name
        t['artist_name'] = e.get('master_metadata_album_artist_name', '')
        t['play_count'] += 1
        t['total_ms'] += e['ms_played']
    return tracks


def create_playlist(name, track_uris, description=''):
    # Use POST /me/playlists (Spotify removed /users/{id}/playlists in Feb 2026)
    payload = {'name': name, 'public': True, 'description': description}
    playlist = sp._post('me/playlists', payload=payload)
    # Use /playlists/{id}/items (Spotify removed /tracks in Feb 2026)
    for i in range(0, len(track_uris), 100):
        sp._post(f"playlists/{playlist['id']}/items", payload={'uris': track_uris[i:i + 100]})
    return playlist['external_urls']['spotify']


def main():
    print('Loading streaming history...')
    entries = load_streaming_history()
    genre_cache = load_genre_cache()

    # --- Playlist 1: Top 100 songs, last 10 years (2016-2026) ---
    recent = [e for e in entries if e.get('ts', '') >= '2016-01-01']
    print(f'Last 10 years: {len(recent):,} entries')

    recent_tracks = aggregate_tracks(recent)
    top_100_recent = sorted(recent_tracks.items(), key=lambda x: x[1]['play_count'], reverse=True)[:100]

    print('\nTOP 100 TRACKS (2016-2026):')
    for i, (uri, t) in enumerate(top_100_recent, 1):
        print(f"  {i:3}. {t['track_name']} — {t['artist_name']} ({t['play_count']} plays)")

    # --- Playlist 2: Top 100 DnB songs, all time ---
    DNB_EXCLUDE = {'Lane 8', 'The Kite String Tangle', 'BCee feat. Rocky Nti', 'BCee'}
    dnb_artists = set()
    for artist_name, genres in genre_cache.items():
        if any('drum and bass' in g.lower() for g in genres):
            if artist_name not in DNB_EXCLUDE:
                dnb_artists.add(artist_name)
    print(f'\nDnB artists found in genre cache: {len(dnb_artists)}')

    all_tracks = aggregate_tracks(entries)
    dnb_tracks = {uri: t for uri, t in all_tracks.items() if t['artist_name'] in dnb_artists}
    top_100_dnb = sorted(dnb_tracks.items(), key=lambda x: x[1]['play_count'], reverse=True)[:100]

    print(f'\nTOP 100 DnB TRACKS (all time):')
    for i, (uri, t) in enumerate(top_100_dnb, 1):
        print(f"  {i:3}. {t['track_name']} — {t['artist_name']} ({t['play_count']} plays)")

    # --- Create playlists ---
    print('\nCreating playlists on Spotify...')

    url1 = create_playlist(
        'My Top 100 (2016–2026)',
        [uri for uri, _ in top_100_recent],
        description='Top 100 most-played tracks from the last 10 years. Generated from streaming history.',
    )
    print(f'  Created: {url1}')

    url2 = create_playlist(
        'My Top 100 Drum & Bass',
        [uri for uri, _ in top_100_dnb],
        description='Top 100 most-played drum and bass tracks, all time. Generated from streaming history.',
    )
    print(f'  Created: {url2}')


if __name__ == '__main__':
    main()
