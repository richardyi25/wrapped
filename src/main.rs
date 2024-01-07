use std::{fs::{read_dir, read_to_string}, collections::HashMap};
use serde::{Serialize, Deserialize};
use sqlx::{QueryBuilder, MySql, query_builder::Separated};
use itertools::Itertools;

#[derive(Debug, Serialize, Deserialize)]
struct SpotifyStream {
    ts: String,
    username: String,
    platform: String,
    ms_played: i32,
    conn_country: String,
    ip_addr_decrypted: Option<String>,
    user_agent_decrypted: Option<String>,
    master_metadata_track_name: Option<String>,
    master_metadata_album_artist_name: Option<String>,
    master_metadata_album_album_name: Option<String>,
    spotify_track_uri: Option<String>,
    episode_name: Option<String>,
    episode_show_name: Option<String>,
    spotify_episode_uri: Option<String>,
    reason_start: String,
    reason_end: Option<String>,
    shuffle: Option<bool>,
    skipped: Option<bool>,
    offline: Option<bool>,
    offline_timestamp: Option<i64>,
    incognito_mode: Option<bool>,
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyAccessToken {
    access_token: String,
    token_type: String,
    expires_in: i32,
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyExternalUrl {
    spotify: Option<String>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyImageObject {
    url: String,
    height: Option<i32>,
    width: Option<i32>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyRestriction {
    reason: Option<String>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifySimplifiedArtist {
    external_urls: SpotifyExternalUrl,
    href: String,
    id: String,
    name: String,
    r#type: String,
    uri: String
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyAlbum {
    album_type: String,
    total_tracks: i32,
    available_markets: Vec<String>,
    external_urls: SpotifyExternalUrl,
    href: String,
    id: String,
    images: Vec<SpotifyImageObject>,
    name: String,
    release_date: String,
    release_date_precision: String,
    restrictions: Option<SpotifyRestriction>,
    r#type: String,
    uri: String,
    artists: Vec<SpotifySimplifiedArtist>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyFollowers {
    href: Option<String>,
    total: i32
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyArtist {
    external_urls: Option<SpotifyExternalUrl>,
    followers: Option<SpotifyFollowers>,
    genres: Option<Vec<String>>,
    href: Option<String>,
    id: Option<String>,
    images: Option<Vec<SpotifyImageObject>>,
    name: Option<String>,
    popularity: Option<i32>,
    r#type: Option<String>,
    uri: Option<String>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyExternalIds {
    isrc: Option<String>,
    ean: Option<String>,
    upc: Option<String>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyLinkedFrom { }

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyTrack {
    album: Option<SpotifyAlbum>,
    artists: Option<Vec<SpotifyArtist>>,
    available_markets: Option<Vec<String>>,
    disc_number: Option<i32>,
    duration_ms: Option<i32>,
    explicit: Option<bool>,
    external_ids: Option<SpotifyExternalIds>,
    external_urls: Option<SpotifyExternalUrl>,
    href: Option<String>,
    id: Option<String>,
    is_playable: Option<bool>,
    linked_from: Option<SpotifyLinkedFrom>,
    restrictions: Option<SpotifyRestriction>,
    name: Option<String>,
    popularity: Option<i32>,
    preview_url: Option<String>,
    track_number: Option<i32>,
    r#type: Option<String>,
    uri: Option<String>,
    is_local: Option<bool>
}


#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyMultipleTracks {
    tracks: Vec<SpotifyTrack>
}

#[allow(dead_code)]
#[derive(Debug,Deserialize)]
struct SpotifyMultipleArtists {
    artists: Vec<SpotifyArtist>
}

#[derive(Debug)]
struct SimplifiedTrack {
    id: String,
    primary_artist_id: Option<String>,
    album_id: Option<String>,
    artist_ids: Vec<String>,
    disc_number: Option<i32>,
    duration_ms: Option<i32>,
    explicit: Option<bool>,
    name: Option<String>,
    popularity: Option<i32>,
    track_number: Option<i32>,
    r#type: Option<String>,
    is_local: Option<bool>
}

#[derive(Debug, Clone)]
struct SimplifiedAlbum {
    album_type: String,
    total_tracks: i32,
    id: String,
    name: String,
    release_date: String,
    release_date_precision: String,
    artists: Vec<String>
}

#[derive(Debug, Clone)]
struct SimplifiedArtist {
    followers: Option<i32>,
    genres: Option<Vec<String>>,
    id: String,
    name: Option<String>,
    popularity: Option<i32>,
    r#type: Option<String>,
}

async fn bulk_insert<'a, T, F>(pool: &sqlx::Pool<sqlx::MySql>, table: &str, elements: &'a [T], row_closure: F)
where F: Fn(Separated<'_, 'a, MySql, &str>, &'a T), T: core::fmt::Debug {
    const SQL_BATCH_SIZE: usize = 1000;

    for chunk in elements.chunks(SQL_BATCH_SIZE) {
        let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(format!("INSERT INTO {} ", table));
        query_builder.push_values(chunk, &row_closure);
        query_builder.build().execute(pool).await.unwrap_or_else(|e| {
            println!("{}", e);
            println!("{}", table);
            println!("{:?}", chunk);
            panic!();
        });
    }
}

async fn load_raw_data(pool: &sqlx::Pool<sqlx::MySql>) {
    let _: Vec<(String,)> = sqlx::query_as("DELETE FROM streams").fetch_all(pool).await.unwrap();

    for path in read_dir("../history/").unwrap() {
        let str = read_to_string(path.as_ref().unwrap().path()).unwrap();
        let rows: Vec<SpotifyStream> = serde_json::from_str(&str).unwrap();

        bulk_insert(pool, "streams", &rows, |mut row, stream| {
            row
            .push(format!("STR_TO_DATE('{}', '%Y-%m-%dT%H:%i:%sZ')", &stream.ts))
            .push_bind(&stream.username)
            .push_bind(&stream.platform)
            .push_bind( stream.ms_played)
            .push_bind(&stream.conn_country)
            .push_bind(&stream.ip_addr_decrypted)
            .push_bind(&stream.user_agent_decrypted)
            .push_bind(&stream.master_metadata_track_name)
            .push_bind(&stream.master_metadata_album_artist_name)
            .push_bind(&stream.master_metadata_album_album_name)
            .push_bind(&stream.spotify_track_uri)
            .push_bind(&stream.episode_name)
            .push_bind(&stream.episode_show_name)
            .push_bind(&stream.spotify_episode_uri)
            .push_bind(&stream.reason_start)
            .push_bind(&stream.reason_end)
            .push_bind( stream.shuffle)
            .push_bind( stream.skipped)
            .push_bind( stream.offline)
            .push_bind( stream.offline_timestamp)
            .push_bind( stream.incognito_mode);
        }).await;
    }
}

async fn get_new_token() -> String {
    let client_id = read_to_string(".client_id").unwrap();
    let client_secret = read_to_string(".client_string").unwrap();

    reqwest::Client::new().post("https://accounts.spotify.com/api/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str())
        ])
        .send().await.unwrap()
        .json::<SpotifyAccessToken>().await.unwrap().access_token
}

async fn spotify_request(token: &mut String, url: reqwest::Url) -> String {
    let mut backoff = 1;

    loop {
        let request = reqwest::Client::new().get(url.clone()).bearer_auth(&token);
        let request_copy = request.try_clone().unwrap();
        let response = request_copy.send().await.unwrap();
        let status = response.status();

        if !matches!(status, reqwest::StatusCode::TOO_MANY_REQUESTS) {
            backoff = 1;
        }

        match status {
            reqwest::StatusCode::UNAUTHORIZED => {
                println!("Invalid token! Getting a new one...");
                *token = get_new_token().await;
            }
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = response.headers().get("Retry-After");
                let timeout = match retry_after {
                    Some(value) => {
                        value.to_str().unwrap().to_owned().parse().unwrap()
                    }
                    None => backoff
                };
                println!("Too many requests! Backing off {timeout} second(s)...");
                tokio::time::sleep(std::time::Duration::from_secs(timeout)).await;
                backoff *= 2;
            }
            reqwest::StatusCode::OK => {
                return response.text().await.unwrap();
            }
            _ => {
                panic!("Unhandled return code {}!", status);
            }
        }
    }
}

async fn get_track_info(token: &mut String, pool: &sqlx::Pool<sqlx::MySql>) {
    for table in ["tracks_artists", "albums_artists", "artists_genres", "tracks", "albums", "artists"] {
        let _: Vec<(String,)> = sqlx::query_as(format!("DELETE FROM {}", table).as_str()).fetch_all(pool).await.unwrap();
    }

    const MAX_TRACK_IDS: usize = 50;
    let tuple_rows: Vec<(String,)> = sqlx::query_as(
    "SELECT DISTINCT spotify_track_uri FROM streams WHERE spotify_track_uri IS NOT NULL")
    .fetch_all(pool).await.unwrap();
	let rows = tuple_rows.iter().map(|t|
        // Remove the first 14 characters, corresponding to "spotify:track:"
        // to convert from Spotify URI to track ID
        t.0.clone().chars().skip(14).collect::<String>()
    );

    let mut tracks: Vec<SimplifiedTrack> = Vec::new();
    let mut albums: HashMap<String, SimplifiedAlbum> = HashMap::new();
    let mut artists: HashMap<String, SimplifiedArtist> = HashMap::new();

    for row_chunk in rows.chunks(MAX_TRACK_IDS).into_iter() {
        let url = reqwest::Url::parse_with_params("https://api.spotify.com/v1/tracks", &[
            ("ids", row_chunk.into_iter().join(","))
        ]).unwrap();
        let response = spotify_request(token, url).await;
        let s_tracks: SpotifyMultipleTracks = serde_json::from_str(&response).unwrap();
        
        for s_track in s_tracks.tracks {
            tracks.push(SimplifiedTrack {
                id: s_track.id.unwrap(),
                primary_artist_id: s_track.artists.as_ref().and_then(|artists|
                    artists.get(0)).and_then(|artist| artist.id.clone()),
                album_id: s_track.album.as_ref().map(|a| a.id.to_owned()),
                artist_ids: match s_track.artists.as_ref() {
                    Some(artists) => artists.iter().filter_map(|a| a.id.clone()).collect(),
                    None => Vec::new()
                },
                disc_number: s_track.disc_number,
                duration_ms: s_track.duration_ms,
                explicit: s_track.explicit,
                name: s_track.name,
                popularity: s_track.popularity,
                track_number: s_track.track_number,
                r#type: s_track.r#type,
                is_local: s_track.is_local
            });

            if let Some(s_album) = s_track.album {
                albums.insert(s_album.id.clone(), SimplifiedAlbum {
                    album_type: s_album.album_type,
                    total_tracks: s_album.total_tracks,
                    id: s_album.id.clone(),
                    name: s_album.name,
                    release_date: s_album.release_date,
                    release_date_precision: s_album.release_date_precision,
                    artists: s_album.artists.iter().map(|a| a.id.clone()).collect()
                });
            }

            if let Some(s_artists) = s_track.artists {
                for s_artist in s_artists {
                    if let Some(s_artist_id) = s_artist.id {
                        artists.insert(s_artist_id.clone(), SimplifiedArtist {
                            followers: s_artist.followers.map(|f| f.total),
                            genres: s_artist.genres,
                            id: s_artist_id.clone(),
                            popularity: s_artist.popularity,
                            name: s_artist.name,
                            r#type: s_artist.r#type
                        });
                    }
                }
            }
        }
    }

    bulk_insert(pool, "tracks", &tracks, |mut row, track| {
        row
        .push_bind(&track.id)
        .push_bind(&track.album_id)
        .push_bind(&track.primary_artist_id)
        .push_bind( track.disc_number)
        .push_bind( track.duration_ms)
        .push_bind( track.explicit)
        .push_bind(&track.name)
        .push_bind( track.popularity)
        .push_bind( track.track_number)
        .push_bind(&track.r#type)
        .push_bind( track.is_local);
    }).await;

    bulk_insert(pool, "albums",
    &albums.values().cloned().collect::<Vec<SimplifiedAlbum>>(),
    |mut row, album| {
        row
        .push_bind(&album.id)
        .push_bind(&album.album_type)
        .push_bind( album.total_tracks)
        .push_bind(&album.name)
        .push_bind(&album.release_date)
        .push_bind(&album.release_date_precision);
    }).await;

    bulk_insert(pool, "artists",
    &artists.values().cloned().collect::<Vec<SimplifiedArtist>>(),
    |mut row, artist| {
        row
        .push_bind(&artist.id)
        .push_bind( artist.followers)
        .push_bind(&artist.name)
        .push_bind( artist.popularity)
        .push_bind(&artist.r#type);
    }).await;


    let mut track_artist: Vec<(String, String)> = Vec::new();
    for track in tracks {
        for artist in track.artist_ids {
            track_artist.push((track.id.clone(), artist));
        }
    }
    bulk_insert(pool, "tracks_artists", &track_artist, |mut row, track_artist_pair| {
        row
        .push_bind(&track_artist_pair.0)
        .push_bind(&track_artist_pair.1);
    }).await;

    let mut albums_artists: Vec<(String, String)> = Vec::new();
    for (id, album) in albums {
        for artist in album.artists {
            albums_artists.push((id.clone(), artist));
        }
    }
    bulk_insert(pool, "albums_artists", &albums_artists, |mut row, album_artist_pair| {
        row
        .push_bind(&album_artist_pair.0)
        .push_bind(&album_artist_pair.1);
    }).await;

    let mut artists_genres: Vec<(String, String)> = Vec::new();
    for (id, artist) in artists {
        if let Some(genres) = artist.genres {
            for genre in genres {
                artists_genres.push((id.clone(), genre));
            }
        }
    }
    bulk_insert(pool, "artists_genres", &artists_genres, |mut row, artist_genre_pair| {
        row
        .push_bind(&artist_genre_pair.0)
        .push_bind(&artist_genre_pair.1);
    }).await;
}

async fn fill_artist_data(token: &mut String, pool: &sqlx::Pool<sqlx::MySql>) {
    let artist_ids: Vec<(String,)> = sqlx::query_as(
    "SELECT DISTINCT id FROM artists")
    .fetch_all(pool).await.unwrap();

    const MAX_SPOTIFY_ARTISTS: usize = 50;

    for chunk in artist_ids.chunks(MAX_SPOTIFY_ARTISTS) {
        let response = spotify_request(token, reqwest::Url::parse_with_params("https://api.spotify.com/v1/artists", &[
            ("ids", chunk.iter().map(|t| &t.0).join(","))
        ]).unwrap()).await;

        let artists: SpotifyMultipleArtists = serde_json::from_str(&response).unwrap();
        for artist in artists.artists {
            let _: Vec<(String,)> = sqlx::query_as("UPDATE artists SET followers = ?, popularity = ? WHERE id = ?")
            .bind(artist.followers.map(|f| f.total))
            .bind(artist.popularity)
            .bind(artist.id)
            .fetch_all(pool).await.unwrap();
        }
    }
}

#[tokio::main]
async fn main() {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect("mysql://richard:passw0rd@localhost/wrapped").await.unwrap();

    let mut token = get_new_token().await;
    
    load_raw_data(&pool).await;
    get_track_info(&mut token, &pool).await;
    fill_artist_data(&mut token, &pool).await;
}