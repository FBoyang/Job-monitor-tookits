"""Discord webhook notifier for SLURM job status."""

import json
import requests
from datetime import datetime


class DiscordNotifier:
    def __init__(self, success_webhook: str, error_webhook: str):
        self.success_webhook = success_webhook
        self.error_webhook = error_webhook

    def notify(self, job_id: str, sacct_info: dict, analysis: dict):
        """Send job completion notification to the appropriate Discord channel."""
        is_success = (
            sacct_info.get("state", "").startswith("COMPLETED")
            and sacct_info.get("exit_code", "") == "0:0"
        )
        webhook = self.success_webhook if is_success else self.error_webhook
        payload = self._build_payload(job_id, sacct_info, analysis, is_success)

        for attempt in range(3):
            try:
                resp = requests.post(webhook, json=payload, timeout=15)
                if resp.status_code == 204:
                    return True
                resp.raise_for_status()
                return True
            except requests.RequestException as e:
                if attempt == 2:
                    print(f"[jobmon] Failed to send Discord notification after 3 attempts: {e}")
                    return False
        return False

    def _build_payload(self, job_id: str, sacct_info: dict, analysis: dict, is_success: bool) -> dict:
        if is_success:
            color = 0x2ECC71  # green
            title = f"\u2705 Job `{job_id}` COMPLETED"
        else:
            color = 0xE74C3C  # red
            state = sacct_info.get("state", "UNKNOWN")
            title = f"\u274C Job `{job_id}` {state}"

        fields = [
            {"name": "Job ID", "value": f"`{job_id}`", "inline": True},
            {"name": "State", "value": sacct_info.get("state", "N/A"), "inline": True},
            {"name": "Exit Code", "value": sacct_info.get("exit_code", "N/A"), "inline": True},
            {"name": "Elapsed", "value": sacct_info.get("elapsed", "N/A"), "inline": True},
            {"name": "Job Name", "value": sacct_info.get("job_name", "N/A"), "inline": True},
            {"name": "Max RSS", "value": sacct_info.get("max_rss", "N/A") or "N/A", "inline": True},
        ]

        if not is_success and analysis.get("has_errors"):
            fields.append({
                "name": "Errors Detected",
                "value": analysis.get("error_summary", "Unknown error")[:1024],
                "inline": False,
            })
            relevant = analysis.get("relevant_lines", [])
            if relevant:
                error_text = "\n".join(relevant[:8])
                if len(error_text) > 1024:
                    error_text = error_text[:1021] + "..."
                fields.append({
                    "name": "Relevant Error Lines",
                    "value": f"```\n{error_text}\n```",
                    "inline": False,
                })

        if not is_success:
            tail = analysis.get("tail", [])
            if tail:
                tail_text = "\n".join(tail[-10:])
                if len(tail_text) > 1024:
                    tail_text = tail_text[:1021] + "..."
                fields.append({
                    "name": "Last Lines of Stderr",
                    "value": f"```\n{tail_text}\n```",
                    "inline": False,
                })

        embed = {
            "title": title,
            "color": color,
            "fields": fields,
            "footer": {"text": "SLURM Job Monitor (jobmon)"},
            "timestamp": datetime.utcnow().isoformat(),
        }

        return {"embeds": [embed]}

    def send_test(self):
        """Send test messages to both channels."""
        test_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        results = {}

        # Test success channel
        payload = {
            "embeds": [{
                "title": "\u2705 jobmon test — Success Channel",
                "description": f"Test message sent at {test_time}",
                "color": 0x2ECC71,
                "footer": {"text": "SLURM Job Monitor (jobmon)"},
            }]
        }
        try:
            resp = requests.post(self.success_webhook, json=payload, timeout=15)
            results["success_channel"] = resp.status_code in (200, 204)
        except requests.RequestException as e:
            results["success_channel"] = False
            results["success_error"] = str(e)

        # Test error channel
        payload = {
            "embeds": [{
                "title": "\u274C jobmon test — Error Channel",
                "description": f"Test message sent at {test_time}",
                "color": 0xE74C3C,
                "footer": {"text": "SLURM Job Monitor (jobmon)"},
            }]
        }
        try:
            resp = requests.post(self.error_webhook, json=payload, timeout=15)
            results["error_channel"] = resp.status_code in (200, 204)
        except requests.RequestException as e:
            results["error_channel"] = False
            results["error_error"] = str(e)

        return results
