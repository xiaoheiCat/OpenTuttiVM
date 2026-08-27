package api

import (
	"net/http"
	"strings"
)

// handleSharePage serves the minimal join page: enter the room password,
// receive a one-time join ticket, and hand off to the desktop app via the
// open-tutti:// deep link. The password never appears in any URL.
func (s *Server) handleSharePage(w http.ResponseWriter, r *http.Request) {
	shareID := r.PathValue("shareID")
	if _, err := s.repo.GetRoomByShareID(r.Context(), shareID); err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("<!doctype html><title>OpenTuttiVM</title><p>This share link is invalid or the meeting has ended.</p>"))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	page := strings.ReplaceAll(sharePageTemplate, "{{SHARE_ID}}", jsEscape(shareID))
	page = strings.ReplaceAll(page, "{{SERVER}}", jsEscape(s.cfg.PublicURL))
	w.Write([]byte(page))
}

func jsEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "<", `\u003c`)
	s = strings.ReplaceAll(s, ">", `\u003e`)
	return s
}

const sharePageTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join OpenTuttiVM Room</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center;background:#0f1115;color:#e7eaf0;margin:0}
.card{background:#171a21;border:1px solid #2a2f3a;border-radius:12px;padding:32px;width:min(360px,90vw)}
h1{font-size:18px;margin:0 0 8px}
p.sub{color:#9aa3b2;font-size:13px;margin:0 0 20px}
input{width:100%;box-sizing:border-box;font-size:18px;letter-spacing:8px;text-align:center;padding:10px;border-radius:8px;border:1px solid #2a2f3a;background:#0f1115;color:#e7eaf0}
button{width:100%;margin-top:14px;padding:10px;border:0;border-radius:8px;background:#4c7dff;color:#fff;font-size:15px;cursor:pointer}
#msg{margin-top:12px;font-size:13px;color:#ff8a8a;min-height:18px}
</style>
</head>
<body>
<main class="card">
<h1>Join OpenTuttiVM Room</h1>
<p class="sub">Enter the 6-digit room password to open this room in Open Tutti.</p>
<form id="f">
<input id="pw" inputmode="numeric" autocomplete="one-time-code" maxlength="6" placeholder="••••••" autofocus>
<button type="submit">Open in Open Tutti</button>
</form>
<div id="msg" role="alert"></div>
<script>
const shareID = "{{SHARE_ID}}", server = "{{SERVER}}";
document.getElementById("f").addEventListener("submit", async (e) => {
  e.preventDefault();
  const msg = document.getElementById("msg");
  msg.textContent = "";
  try {
    const res = await fetch("/api/share/" + shareID + "/join-ticket", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({password: document.getElementById("pw").value})
    });
    const data = await res.json();
    if (!res.ok) { msg.textContent = data.error || "Cannot join this room."; return; }
    window.location.href = "open-tutti://join?server=" + encodeURIComponent(server) + "&ticket=" + encodeURIComponent(data.ticket);
  } catch (err) {
    msg.textContent = "Network error. Try again.";
  }
});
</script>
</main>
</html>
`
