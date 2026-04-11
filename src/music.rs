use rodio::{Decoder, OutputStream, OutputStreamHandle, Sink};
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};

const MUSIC_DIR: &str = "music";

#[derive(Clone)]
pub struct TrackInfo {
    pub path: PathBuf,
    pub name: String,
}

struct PlayerInner {
    _stream: OutputStream,
    _stream_handle: OutputStreamHandle,
    sink: Sink,
}

pub struct MusicPlayer {
    inner: Option<PlayerInner>,
    tracks: Vec<TrackInfo>,
    current_index: Option<usize>,
    volume: f32,
    shuffle: bool,
    shuffle_order: Vec<usize>,
    shuffle_pos: usize,
}

impl MusicPlayer {
    pub fn new() -> Self {
        let tracks = Self::scan_tracks();
        let (inner, volume) = match OutputStream::try_default() {
            Ok((stream, handle)) => {
                let sink = Sink::try_new(&handle).ok();
                match sink {
                    Some(sink) => {
                        sink.set_volume(0.5);
                        (
                            Some(PlayerInner {
                                _stream: stream,
                                _stream_handle: handle,
                                sink,
                            }),
                            0.5,
                        )
                    }
                    None => (None, 0.5),
                }
            }
            Err(_) => (None, 0.5),
        };

        let track_count = tracks.len();
        Self {
            inner,
            tracks,
            current_index: None,
            volume,
            shuffle: false,
            shuffle_order: (0..track_count).collect(),
            shuffle_pos: 0,
        }
    }

    fn scan_tracks() -> Vec<TrackInfo> {
        let music_path = Path::new(MUSIC_DIR);
        let mut tracks = Vec::new();

        if let Ok(entries) = fs::read_dir(music_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let ext = path
                        .extension()
                        .and_then(|e| e.to_str())
                        .unwrap_or("")
                        .to_lowercase();
                    if matches!(ext.as_str(), "mp3" | "ogg" | "wav" | "flac") {
                        let name = path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("Unknown")
                            .to_owned();
                        tracks.push(TrackInfo { path, name });
                    }
                }
            }
        }

        tracks.sort_by(|a, b| a.name.cmp(&b.name));
        tracks
    }

    pub fn tracks(&self) -> &[TrackInfo] {
        &self.tracks
    }

    pub fn current_index(&self) -> Option<usize> {
        self.current_index
    }

    pub fn current_track_name(&self) -> Option<&str> {
        self.current_index
            .and_then(|i| self.tracks.get(i))
            .map(|t| t.name.as_str())
    }

    pub fn is_playing(&self) -> bool {
        self.inner
            .as_ref()
            .map(|p| !p.sink.is_paused() && !p.sink.empty())
            .unwrap_or(false)
    }

    pub fn is_paused(&self) -> bool {
        self.inner
            .as_ref()
            .map(|p| p.sink.is_paused())
            .unwrap_or(false)
    }

    pub fn is_stopped(&self) -> bool {
        self.inner
            .as_ref()
            .map(|p| p.sink.empty())
            .unwrap_or(true)
    }

    pub fn volume(&self) -> f32 {
        self.volume
    }

    pub fn set_volume(&mut self, vol: f32) {
        self.volume = vol.clamp(0.0, 1.0);
        if let Some(inner) = &self.inner {
            inner.sink.set_volume(self.volume);
        }
    }

    pub fn shuffle(&self) -> bool {
        self.shuffle
    }

    pub fn set_shuffle(&mut self, enabled: bool) {
        self.shuffle = enabled;
        if enabled {
            self.reshuffle();
        }
    }

    fn reshuffle(&mut self) {
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        self.shuffle_order = (0..self.tracks.len()).collect();
        self.shuffle_order.shuffle(&mut thread_rng());
        self.shuffle_pos = 0;
    }

    pub fn play_track(&mut self, index: usize) {
        if index >= self.tracks.len() {
            return;
        }
        let Some(inner) = &self.inner else { return };

        let path = &self.tracks[index].path;
        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let source = match Decoder::new(BufReader::new(file)) {
            Ok(s) => s,
            Err(_) => return,
        };

        inner.sink.stop();
        inner.sink.clear();
        inner.sink.append(source);
        inner.sink.set_volume(self.volume);
        inner.sink.play();
        self.current_index = Some(index);
    }

    pub fn pause(&self) {
        if let Some(inner) = &self.inner {
            inner.sink.pause();
        }
    }

    pub fn resume(&self) {
        if let Some(inner) = &self.inner {
            inner.sink.play();
        }
    }

    pub fn stop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.sink.clear();
        }
        self.current_index = None;
    }

    pub fn next_track(&mut self) {
        if self.tracks.is_empty() {
            return;
        }
        let next = if self.shuffle {
            self.shuffle_pos = (self.shuffle_pos + 1) % self.shuffle_order.len();
            self.shuffle_order[self.shuffle_pos]
        } else {
            match self.current_index {
                Some(i) => (i + 1) % self.tracks.len(),
                None => 0,
            }
        };
        self.play_track(next);
    }

    pub fn previous_track(&mut self) {
        if self.tracks.is_empty() {
            return;
        }
        let prev = if self.shuffle {
            if self.shuffle_pos == 0 {
                self.shuffle_pos = self.shuffle_order.len().saturating_sub(1);
            } else {
                self.shuffle_pos -= 1;
            }
            self.shuffle_order[self.shuffle_pos]
        } else {
            match self.current_index {
                Some(0) | None => self.tracks.len().saturating_sub(1),
                Some(i) => i - 1,
            }
        };
        self.play_track(prev);
    }

    /// Call once per frame to auto-advance when a track finishes.
    pub fn tick(&mut self) {
        if self.current_index.is_some() && self.is_stopped() {
            self.next_track();
        }
    }
}
