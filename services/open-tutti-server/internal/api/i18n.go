// Server-rendered H5 pages negotiate the visitor's language from
// Accept-Language; user-visible copy lives here instead of being
// hardcoded into templates.
package api

import (
	"strings"
)

// locale carries every user-visible string of the share H5 page.
type locale struct {
	Lang            string
	Title           string
	Heading         string
	Sub             string
	Placeholder     string
	Submit          string
	JoinError       string
	NetworkError    string
	InvalidShareMsg string
}

var shareLocales = map[string]locale{
	"en": {
		Lang:            "en",
		Title:           "Join OpenTuttiVM Room",
		Heading:         "Join OpenTuttiVM Room",
		Sub:             "Enter the 6-digit room password to open this room in Open Tutti.",
		Placeholder:     "••••••",
		Submit:          "Open in Open Tutti",
		JoinError:       "Cannot join this room.",
		NetworkError:    "Network error. Try again.",
		InvalidShareMsg: "This share link is invalid or the meeting has ended.",
	},
	"zh": {
		Lang:            "zh",
		Title:           "加入 OpenTuttiVM 房间",
		Heading:         "加入 OpenTuttiVM 房间",
		Sub:             "输入 6 位房间密码，在 Open Tutti 中打开此房间",
		Placeholder:     "••••••",
		Submit:          "在 Open Tutti 中打开",
		JoinError:       "无法加入此房间",
		NetworkError:    "网络出错，请重试",
		InvalidShareMsg: "分享链接无效，或会议已结束",
	},
}

// negotiateLocale picks the best share-page locale for an Accept-Language
// header; unknown languages fall back to English.
func negotiateLocale(acceptLanguage string) locale {
	for _, part := range strings.Split(acceptLanguage, ",") {
		tag := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if tag == "" || tag == "*" {
			continue
		}
		lang := strings.ToLower(strings.SplitN(tag, "-", 2)[0])
		if l, ok := shareLocales[lang]; ok {
			return l
		}
	}
	return shareLocales["en"]
}
