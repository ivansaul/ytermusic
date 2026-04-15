use std::process::Stdio;

use log::error;
use tokio::process::Command;
use ytpapi2::YoutubeMusicVideoRef;

use crate::{DownloadManager, DownloadManagerMessage, MessageHandler, MusicDownloadStatus};

#[derive(Debug)]
pub enum DownloadError {
    YtDlpFailed(String),
    IoError(std::io::Error),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::YtDlpFailed(msg) => write!(f, "yt-dlp failed: {}", msg),
            DownloadError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(e: std::io::Error) -> Self {
        DownloadError::IoError(e)
    }
}

async fn download_with_ytdlp(
    video_id: &str,
    output_path: &std::path::Path,
    sender: &MessageHandler,
) -> Result<(), DownloadError> {
    sender(DownloadManagerMessage::VideoStatusUpdate(
        video_id.to_string(),
        MusicDownloadStatus::Downloading(0),
    ));

    let url = format!("https://www.youtube.com/watch?v={}", video_id);

    let output = Command::new("yt-dlp")
        .args([
            "--no-playlist",
            "-f", "bestaudio[ext=m4a]/bestaudio[ext=mp4]/bestaudio",
            "--merge-output-format", "mp4",
            "-o", output_path.to_str().unwrap(),
            "--no-progress",
            "--quiet",
            &url,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(DownloadError::YtDlpFailed(stderr));
    }

    sender(DownloadManagerMessage::VideoStatusUpdate(
        video_id.to_string(),
        MusicDownloadStatus::Downloading(100),
    ));

    Ok(())
}

impl DownloadManager {
    async fn handle_download(
        &self,
        id: &str,
        sender: MessageHandler,
    ) -> Result<(), DownloadError> {
        let file = self.cache_dir.join("downloads").join(format!("{id}.mp4"));
        download_with_ytdlp(id, &file, &sender).await
    }

    pub async fn start_download(&self, song: YoutubeMusicVideoRef, s: MessageHandler) -> bool {
        {
            let mut downloads = self.in_download.lock().unwrap();
            if downloads.contains(&song.video_id) {
                return false;
            }
            downloads.insert(song.video_id.clone());
        }
        s(DownloadManagerMessage::VideoStatusUpdate(
            song.video_id.clone(),
            MusicDownloadStatus::Downloading(1),
        ));
        let download_path_mp4 = self
            .cache_dir
            .join(format!("downloads/{}.mp4", &song.video_id));
        let download_path_json = self
            .cache_dir
            .join(format!("downloads/{}.json", &song.video_id));
        if download_path_json.exists() {
            s(DownloadManagerMessage::VideoStatusUpdate(
                song.video_id.clone(),
                MusicDownloadStatus::Downloaded,
            ));
            return true;
        }
        if download_path_mp4.exists() {
            std::fs::remove_file(&download_path_mp4).unwrap();
        }
        match self.handle_download(&song.video_id, s.clone()).await {
            Ok(_) => {
                std::fs::write(download_path_json, serde_json::to_string(&song).unwrap()).unwrap();
                self.database.append(song.clone());
                s(DownloadManagerMessage::VideoStatusUpdate(
                    song.video_id.clone(),
                    MusicDownloadStatus::Downloaded,
                ));
                self.in_download.lock().unwrap().remove(&song.video_id);
                true
            }
            Err(e) => {
                if download_path_mp4.exists() {
                    std::fs::remove_file(download_path_mp4).unwrap();
                }
                s(DownloadManagerMessage::VideoStatusUpdate(
                    song.video_id.clone(),
                    MusicDownloadStatus::DownloadFailed,
                ));
                error!("Error downloading {}: {e}", song.video_id);
                false
            }
        }
    }

    pub fn start_task_unary(
        &'static self,
        s: MessageHandler,
        song: YoutubeMusicVideoRef,
        cancelation: impl Future<Output = ()> + Send + 'static,
    ) {
        let fut = async move {
            self.start_download(song, s).await;
        };
        let service = tokio::task::spawn(async move {
            tokio::select! {
                _ = fut => {},
                _ = cancelation => {},
            }
        });
        self.handles.lock().unwrap().push(service);
    }
}
