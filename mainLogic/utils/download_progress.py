"""Professional download progress UI using Rich library."""
import sys
import time

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
        TransferSpeedColumn,
        DownloadColumn,
    )
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Progress = None
    Console = None


def _progress_bar(percent, width=30):
    """Fallback ASCII progress bar."""
    filled = int((percent / 100) * width)
    return "█" * filled + "░" * (width - filled)


def _format_eta(seconds):
    """Format ETA in human-readable format."""
    if seconds is None or seconds <= 0:
        return "--:--"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}:{secs:02d}"


def make_download_progress(title=None, use_rich=True):
    """
    Create a professional download progress tracker.
    
    Args:
        title: Title to display (e.g., lecture name)
        use_rich: Whether to use Rich library (if available)
    
    Returns:
        dict with 'update' and 'finish' callbacks
    """
    state = {
        "last_ts": 0.0,
        "last_pct": -1,
        "last_bytes": 0,
        "rich": None,
        "start_time": time.monotonic(),
    }
    
    use_rich = use_rich and RICH_AVAILABLE
    
    if use_rich:
        display_title = title or "Download"
        if isinstance(display_title, str) and len(display_title) > 60:
            display_title = f"...{display_title[-57:]}"
        
        progress = Progress(
            TextColumn("[bold cyan]⬇[/bold cyan] {task.fields[title]}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            expand=True,
        )
        progress.start()
        task_id = progress.add_task("download", total=1, title=display_title)
        state["rich"] = (progress, task_id)
    
    def update(current, total):
        """
        Update progress.
        
        Args:
            current: Current bytes downloaded
            total: Total bytes to download
        """
        if not total or total <= 0:
            return
        
        pct = int((current / total) * 100)
        now = time.monotonic()
        
        # Throttle updates (max 2x per second)
        if pct == state["last_pct"] and (now - state["last_ts"]) < 0.5:
            return
        
        elapsed = now - state["last_ts"] if state["last_ts"] else None
        speed_bps = None
        if elapsed and elapsed > 0:
            speed_bps = (current - state["last_bytes"]) / elapsed
        
        state["last_pct"] = pct
        state["last_ts"] = now
        state["last_bytes"] = current
        
        if state["rich"]:
            progress, task_id = state["rich"]
            progress.update(
                task_id,
                completed=current,
                total=total,
            )
        else:
            # Fallback ASCII progress bar
            sent_mb = current / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            speed_mb = (speed_bps / (1024 * 1024)) if speed_bps and speed_bps > 0 else 0.0
            eta = None
            if speed_bps and speed_bps > 0:
                eta = (total - current) / speed_bps
            
            bar = _progress_bar(pct)
            sys.stdout.write(
                f"\r⬇️ Downloading {bar} {pct:3d}% | {speed_mb:.2f} MB/s | ETA {_format_eta(eta)} | {sent_mb:.1f}/{total_mb:.1f} MB"
            )
            sys.stdout.flush()
    
    def finish():
        """Mark download as complete and clean up."""
        if state["rich"]:
            progress, task_id = state["rich"]
            progress.stop()
            state["rich"] = None
        else:
            sys.stdout.write("\r" + " " * 120 + "\r")
            sys.stdout.flush()
        
        total_time = time.monotonic() - state["start_time"]
        return total_time
    
    return {"update": update, "finish": finish}


# Example usage for testing
if __name__ == "__main__":
    import random
    
    tracker = make_download_progress("Test Video.mp4")
    total = 100 * 1024 * 1024  # 100 MB
    
    for i in range(100):
        current = int(total * (i + 1) / 100)
        tracker["update"](current, total)
        time.sleep(0.05)
    
    elapsed = tracker["finish"]()
    print(f"\nDownload completed in {elapsed:.2f}s")
