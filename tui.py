import os
import time
from datetime import datetime
from threading import Lock
from typing import Dict, List, Optional, Callable
from pathlib import Path
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.live import Live
from rich.table import Table
from rich.text import Text
import concurrent.futures

from mainLogic.big4 import Gryffindor_downloadv3
from mainLogic.big4.Gryffindor_downloadv3 import DownloadResult


class DownloaderTUI:
    """Advanced Terminal User Interface for the Downloader using rich library"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.console = Console()
        self.lock = Lock()
        self.log_messages: List[Dict] = []
        self.max_log_lines = 100
        self.status = "Ready"

        # Progress trackers
        self.audio_progress = None
        self.audio_task_id = None
        self.video_progress = None
        self.video_task_id = None

        # Stats
        self.audio_stats = {"total": 0, "successful": 0, "failed": []}
        self.video_stats = {"total": 0, "successful": 0, "failed": []}
        self.upload_stats = {"total": 100, "completed": 0, "active": False}
        self.download_start_time = None

        # Layout elements
        self.layout = self._make_layout()
        self.progress_container = Progress()
        self.live = None

    def _make_layout(self) -> Layout:
        """Create the layout structure for the TUI"""
        layout = Layout(name="root")

        # Split the screen into top and bottom sections
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )

        # Split the main section into status and logs
        layout["main"].split_row(
            Layout(name="status", ratio=1),
            Layout(name="logs", ratio=2)
        )

        # Split the status section into progress and stats
        layout["status"].split(
            Layout(name="progress", ratio=1),
            Layout(name="stats", ratio=1)
        )

        return layout

    def start(self):
        """Initialize and start the live display"""
        self.download_start_time = time.time()

        # Initialize progress tracking
        self.progress_container = Progress()

        # Create the live display
        self.live = Live(
            self._generate_layout(),
            refresh_per_second=4,
            console=self.console
        )
        self.live.start()

    def stop(self):
        """Stop the live display"""
        if self.live:
            self.live.stop()

    def log(self, message: str, level: str = "INFO"):
        """Add a log message"""
        with self.lock:
            timestamp = datetime.utcnow().strftime("%H:%M:%S")

            # Set color based on level
            if level == "ERROR":
                color = "red"
            elif level == "WARNING":
                color = "yellow"
            elif level == "DEBUG":
                color = "blue"
            else:
                color = "green"

            self.log_messages.append({
                "timestamp": timestamp,
                "level": level,
                "message": message,
                "color": color
            })

            # Keep logs to the maximum number
            while len(self.log_messages) > self.max_log_lines:
                self.log_messages.pop(0)

        # Update display OUTSIDE the lock to prevent blocking
        self._update_display()

    def set_status(self, status: str):
        """Set a short status line shown in the footer"""
        with self.lock:
            self.status = status
        # Update display OUTSIDE the lock
        self._update_display()

    def _update_display(self):
        """Update the live display with current state"""
        if self.live:
            self.live.update(self._generate_layout())

    def _generate_layout(self):
        """Generate the complete layout with current state"""
        # Header
        header_text = Text("Media Downloader", style="bold white on blue")
        elapsed = ""
        if self.download_start_time:
            elapsed_secs = int(time.time() - self.download_start_time)
            elapsed = f" • Elapsed: {elapsed_secs // 60}m {elapsed_secs % 60}s"
        header_text.append(elapsed, style="white on blue")
        self.layout["header"].update(Panel(header_text, border_style="blue"))

        # Progress section
        progress_panel = self._generate_progress_panel()
        self.layout["progress"].update(progress_panel)

        # Stats section
        stats_panel = self._generate_stats_panel()
        self.layout["stats"].update(stats_panel)

        # Logs section
        logs_panel = self._generate_logs_panel()
        self.layout["logs"].update(logs_panel)

        # Footer
        footer = Table.grid(expand=True)
        footer.add_column(ratio=4)
        footer.add_column(ratio=1, justify="right")
        footer.add_row(Text(self.status or ""), Text("Press Ctrl+C to cancel", style="italic"))
        self.layout["footer"].update(Panel(footer, border_style="grey37"))

        return self.layout

    def _generate_progress_panel(self):
        """Generate the progress panel with progress bars"""
        progress = Progress()

        # Re-create audio progress bar if needed
        if self.audio_stats["total"] > 0:
            audio_task = progress.add_task(
                "[cyan]Audio Download",
                total=self.audio_stats["total"],
                completed=self.audio_stats["successful"] + len(self.audio_stats["failed"])
            )

        # Re-create video progress bar if needed
        if self.video_stats["total"] > 0:
            progress.add_task(
                "[magenta]Video Download",
                total=self.video_stats["total"],
                completed=self.video_stats["successful"] + len(self.video_stats["failed"])
            )

        # Upload progress (0-100)
        if self.upload_stats.get("active"):
            progress.add_task(
                "[yellow]IA Upload",
                total=self.upload_stats.get("total", 100),
                completed=self.upload_stats.get("completed", 0),
            )

        return Panel(progress, title="Download Progress", border_style="green")

    def _generate_stats_panel(self):
        """Generate the stats panel with download statistics"""
        table = Table(show_header=True, header_style="bold")
        table.add_column("Media")
        table.add_column("Total")
        table.add_column("Successful")
        table.add_column("Failed")
        table.add_column("Completion")

        # Add audio stats
        if self.audio_stats["total"] > 0:
            completion = 100 * (self.audio_stats["successful"] + len(self.audio_stats["failed"])) / self.audio_stats["total"]
            table.add_row(
                "Audio",
                str(self.audio_stats["total"]),
                str(self.audio_stats["successful"]),
                str(len(self.audio_stats["failed"])),
                f"{completion:.1f}%"
            )

        # Add video stats
        if self.video_stats["total"] > 0:
            completion = 100 * (self.video_stats["successful"] + len(self.video_stats["failed"])) / self.video_stats["total"]
            table.add_row(
                "Video",
                str(self.video_stats["total"]),
                str(self.video_stats["successful"]),
                str(len(self.video_stats["failed"])),
                f"{completion:.1f}%"
            )

        # Add upload stats
        if self.upload_stats.get("active"):
            uploaded = int(self.upload_stats.get("completed", 0))
            total = int(self.upload_stats.get("total", 100))
            completion = (100 * uploaded / total) if total else 0
            table.add_row(
                "Upload",
                str(total),
                str(uploaded),
                "0",
                f"{completion:.1f}%"
            )

        return Panel(table, title="Download Statistics", border_style="green")

    def _generate_logs_panel(self):
        """Generate the logs panel with colorized log messages"""
        log_text = Text()

        for log in self.log_messages:
            timestamp = log["timestamp"]
            level = log["level"]
            message = log["message"]
            color = log["color"]

            log_text.append(f"[{timestamp}] ", style="dim")
            log_text.append(f"[{level}] ", style=f"bold {color}")
            log_text.append(f"{message}\n", style=color if level != "INFO" else "")

        return Panel(log_text, title="Logs", border_style="blue")

    def setup_audio_progress(self, total_segments: int):
        """Setup the audio progress tracking"""
        with self.lock:
            self.audio_stats["total"] = total_segments
            self.audio_stats["successful"] = 0
            self.audio_stats["failed"] = []
            self.status = f"Preparing audio: {total_segments} segments"
        # Update display OUTSIDE the lock
        self._update_display()

    def setup_video_progress(self, total_segments: int):
        """Setup the video progress tracking"""
        with self.lock:
            self.video_stats["total"] = total_segments
            self.video_stats["successful"] = 0
            self.video_stats["failed"] = []
            self.status = f"Preparing video: {total_segments} segments"
        # Update display OUTSIDE the lock
        self._update_display()

    def update_progress(self, media_type: str, segment_num: int, success: bool):
        """Update progress for a specific media type"""
        with self.lock:
            if media_type == "audio":
                stats = self.audio_stats
            else:  # video
                stats = self.video_stats

            if success:
                stats["successful"] += 1
            else:
                stats["failed"].append(segment_num)

            # Update status briefly
            self.status = f"{media_type.title()}: {stats['successful']}/{stats['total']} ({int(((stats['successful'] + len(stats['failed']))/stats['total'])*100) if stats['total'] else 0}%)"

            # Build return info while holding lock
            result = {
                "type": media_type,
                "total": stats["total"],
                "current": stats["successful"] + len(stats["failed"]),
                "percentage": ((stats["successful"] + len(stats["failed"])) / stats["total"]) * 100 if stats["total"] > 0 else 0,
                "segment_num": segment_num,
                "success": success,
                "failed_segments": stats["failed"].copy(),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }

        # Update display OUTSIDE the lock
        self._update_display()
        return result

    def setup_upload_progress(self):
        """Initialize IA upload progress bar"""
        with self.lock:
            self.upload_stats = {"total": 100, "completed": 0, "active": True}
            self.status = "IA upload started"
        self._update_display()

    def update_upload_progress(self, percent: int, status_msg: Optional[str] = None):
        """Update IA upload progress (0-100)"""
        with self.lock:
            pct = max(0, min(100, int(percent)))
            self.upload_stats["active"] = True
            self.upload_stats["completed"] = pct
            if status_msg:
                self.status = status_msg
            else:
                self.status = f"IA Upload: {pct}%"
        self._update_display()

    def finish_upload_progress(self, success: bool = True):
        """Finalize IA upload progress"""
        with self.lock:
            self.upload_stats["active"] = True
            self.upload_stats["completed"] = 100 if success else self.upload_stats.get("completed", 0)
            self.status = "IA upload done" if success else "IA upload failed"
        self._update_display()

# Modified ProgressTracker to work with the new TUI
class ProgressTracker:
    def __init__(self, total_segments: int, media_type: str, tui: DownloaderTUI, show_tqdm: bool = False):
        self.total = total_segments
        self.current = 0
        self.media_type = media_type
        self.lock = Lock()
        self.failed_segments = []
        self.tui = tui
        self.log_interval = max(1, total_segments // 100)  # Log every 1% of progress

        # Setup progress in TUI
        if media_type == "audio":
            self.tui.setup_audio_progress(total_segments)
        else:
            self.tui.setup_video_progress(total_segments)

        self.tui.log(f"Starting {media_type} download: {total_segments} segments", "INFO")

    def update(self, segment_num: int, success: bool = True) -> Dict:
        with self.lock:
            self.current += 1
            if not success:
                self.failed_segments.append(segment_num)
                # Always log failures
                self.tui.log(f"{self.media_type} segment {segment_num} failed", "ERROR")
            elif self.current % self.log_interval == 0 and self.current > 0:
                # Only log progress every 1% to avoid excessive updates
                percent = int((self.current / self.total) * 100)
                self.tui.log(f"{self.media_type}: {self.current}/{self.total} ({percent}%)", "INFO")

            # Update TUI progress (non-blocking call)
            return self.tui.update_progress(self.media_type, segment_num, success)

    def close(self):
        self.tui.log(f"{self.media_type.capitalize()} download complete: {self.current - len(self.failed_segments)}/{self.total} segments successful", "INFO")

# Example of how to modify the DownloaderV3 class to use the new TUI
def update_downloader_v3_with_tui(downloader:Gryffindor_downloadv3.DownloaderV3, tui: DownloaderTUI = None, manage_lifecycle: bool = True):
    if tui is None:
        tui = DownloaderTUI(downloader.verbose)

    downloader.terminal = tui
    if manage_lifecycle:
        downloader.terminal.start()

    # Override the original _download_media method to use our new ProgressTracker
    #   original_download_media = downloader._download_media

    def _download_media_with_tui(media_data, media_type, output_dir):
        """Download media with TUI progress tracking"""
        downloader.terminal.log(f"{media_type}: Starting", "INFO")
        
        try:
            if not media_data or "segments" not in media_data:
                downloader.terminal.log(f"{media_type}: No data", "WARNING")
                return DownloadResult(None, output_dir, 0, 0, [])

            total_segments = len(media_data["segments"])
            downloader.terminal.log(f"{media_type}: {total_segments} segments", "INFO")
            
            init_file_path = None
            downloader.terminal.set_status(f"{media_type} downloading")
            
            # Match original ProgressTracker initialization
            progress_tracker = ProgressTracker(total_segments, media_type, downloader.terminal, False)

            # Download init segment
            if "init" in media_data:
                init_filename = downloader._get_file_name_from_url(media_data["init"], 0, media_type)
                init_file_path = output_dir / init_filename
                if not downloader._download_segment(media_data["init"], init_file_path):
                    downloader.terminal.log(f"{media_type}: Init failed", "ERROR")
                    return DownloadResult(None, output_dir, total_segments, 0, list(range(1, total_segments + 1)))

            # Build tasks - match original exactly
            download_tasks = []
            for segment_num, segment_url in media_data["segments"].items():
                segment_filename = downloader._get_file_name_from_url(segment_url, int(segment_num), media_type)
                segment_path = output_dir / segment_filename
                download_tasks.append((segment_url, segment_path, int(segment_num), progress_tracker))

            downloader.terminal.log(f"{media_type}: {len(download_tasks)} queued", "INFO")
            
            # Download segments - match original: NO TIMEOUT
            successful_segments = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=downloader.max_workers) as executor:
                futures = [executor.submit(downloader._process_segment, task) for task in download_tasks]

                for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    try:
                        result = future.result()  # NO TIMEOUT - match original
                        if result:
                            successful_segments += 1
                        if i % max(1, len(futures) // 10) == 0 or i == 1:
                            percent = int((i / len(futures)) * 100)
                            msg = f"{media_type}: {i}/{len(futures)} ({percent}%)"
                            downloader.terminal.log(msg, "INFO")
                    except Exception as e:
                        downloader.terminal.log(f"{media_type} error: {str(e)[:60]}", "ERROR")
                        downloader.debugger.error(f"{media_type} segment exception: {e}")

            downloader.terminal.log(f"{media_type}: {successful_segments}/{total_segments}", "INFO")
            progress_tracker.close()
            
            return DownloadResult(init_file_path, output_dir, total_segments, successful_segments, progress_tracker.failed_segments)
        
        except Exception as e:
            downloader.terminal.log(f"_download_media_with_tui EXCEPTION: {str(e)[:150]}", "ERROR")
            downloader.debugger.error(f"_download_media_with_tui error: {e}")
            import traceback
            downloader.terminal.log(f"Traceback: {traceback.format_exc()[:300]}", "ERROR")
            raise

    # Replace the original method with our enhanced version
    # Note: We assign the function directly (not as a bound method) because it closes over 'downloader'
    original_download_media = downloader._download_media
    downloader._download_media = _download_media_with_tui
    downloader.terminal.log("TUI download wrapper active", "INFO")

    # Make sure to stop the TUI when done
    original_download_all = downloader.download_all
    def download_all_with_tui(urls):
        import time
        downloader.terminal.log("Download started", "INFO")
        start_time = time.time()
        try:
            downloader.terminal.set_status("Downloading...")
            result = original_download_all(urls)
            
            elapsed = time.time() - start_time
            downloader.terminal.log(f"Download finished in {elapsed:.1f}s", "INFO")
            return result
        except concurrent.futures.TimeoutError as e:
            elapsed = time.time() - start_time
            downloader.terminal.log(f"Download timeout after {elapsed:.1f}s", "ERROR")
            raise
        except Exception as e:
            elapsed = time.time() - start_time
            downloader.terminal.log(f"Download failed after {elapsed:.1f}s: {str(e)[:100]}", "ERROR")
            raise
        finally:
            downloader.terminal.set_status("Done")
            if manage_lifecycle:
                downloader.terminal.stop()

    downloader.download_all = download_all_with_tui
    downloader.terminal.log(f"Wrapped download_all method", "INFO")

    return downloader

# Usage example:
"""
# Import the original downloader
from mainLogic.downloader import DownloaderV3

# Create the downloader
downloader = DownloaderV3(
    tmp_dir="tmp",
    out_dir="output",
    verbose=True,
    max_workers=8
)

# Apply our TUI enhancements
downloader = update_downloader_v3_with_tui(downloader)

# Use the enhanced downloader
results = downloader.download_all(urls_dict)
"""