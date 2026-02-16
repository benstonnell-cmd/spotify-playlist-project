import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SCOPE = 'playlist-modify-public playlist-modify-private'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    redirect_uri=os.getenv('REDIRECT_URI'),
    scope=SCOPE,
))


def create_playlist(name, track_uris, description='', public=True):
    """Create a Spotify playlist and add tracks to it.

    Args:
        name: Playlist name.
        track_uris: List of Spotify track URIs (e.g. ['spotify:track:6rqhFgbbKwnb9MLmUQDhG6']).
        description: Optional playlist description.
        public: Whether the playlist is public (default True).

    Returns:
        The playlist URL.
    """
    # Use POST /me/playlists (Spotify removed /users/{id}/playlists in Feb 2026)
    payload = {'name': name, 'public': public, 'description': description}
    playlist = sp._post('me/playlists', payload=payload)

    # Use /playlists/{id}/items (Spotify removed /tracks in Feb 2026)
    for i in range(0, len(track_uris), 100):
        sp._post(f"playlists/{playlist['id']}/items", payload={'uris': track_uris[i:i + 100]})

    return playlist['external_urls']['spotify']


if __name__ == '__main__':
    # Example usage — replace with your actual track URIs
    example_uris = [
        'spotify:track:6rqhFgbbKwnb9MLmUQDhG6',
        'spotify:track:3n3Ppam7vgaVa1iaRUc9Lp',
    ]
    url = create_playlist('My Playlist', example_uris, description='Created via script')
    print(f'Playlist created: {url}')
