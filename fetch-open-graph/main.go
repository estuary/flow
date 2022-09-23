package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/net/html"
)

func main() {
	var (
		url = flag.String("url", "", "URL to fetch")
	)
	flag.Parse()

	if *url == "" {
		flag.Usage()
		os.Exit(0)
	}

	// TODO(johnny): Request multiple languages.
	const language = "en-US"

	req, err := http.NewRequest("GET", *url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Accept-Language", language)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	meta := extract(resp.Body)

	var enc = json.NewEncoder(os.Stdout)
	enc.SetIndent(" ", " ")

	if err := enc.Encode(map[string]interface{}{
		language: meta,
	}); err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}

// Credit to https://gist.github.com/inotnako/c4a82f6723f6ccea5d83c5d3689373dd
type HTMLMeta struct {
	Description string `json:"description,omitempty"`
	Image       string `json:"image,omitempty"`
	ImageHeight string `json:"image_height,omitempty"`
	ImageWidth  string `json:"image_width,omitempty"`
	SiteName    string `json:"site_name,omitempty"`
	Title       string `json:"title,omitempty"`
}

func extract(resp io.Reader) *HTMLMeta {
	z := html.NewTokenizer(resp)

	titleFound := false

	hm := new(HTMLMeta)

	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			return hm
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.Data == `body` {
				return hm
			}
			if t.Data == "title" {
				titleFound = true
			}
			if t.Data == "meta" {
				desc, ok := extractMetaProperty(t, "description")
				if ok {
					hm.Description = desc
				}
				ogDesc, ok := extractMetaProperty(t, "og:description")
				if ok {
					hm.Description = ogDesc
				}
				ogImage, ok := extractMetaProperty(t, "og:image")
				if ok {
					hm.Image = ogImage
				}
				ogImageWidth, ok := extractMetaProperty(t, "og:image:width")
				if ok {
					hm.ImageWidth = ogImageWidth
				}
				ogImageHeight, ok := extractMetaProperty(t, "og:image:height")
				if ok {
					hm.ImageHeight = ogImageHeight
				}
				ogSiteName, ok := extractMetaProperty(t, "og:site_name")
				if ok {
					hm.SiteName = ogSiteName
				}
				ogTitle, ok := extractMetaProperty(t, "og:title")
				if ok {
					hm.Title = ogTitle
				}
			}
		case html.TextToken:
			if titleFound {
				t := z.Token()
				hm.Title = strings.TrimSpace(t.Data)
				titleFound = false
			}
		}
	}
}

func extractMetaProperty(t html.Token, prop string) (content string, ok bool) {
	for _, attr := range t.Attr {
		if attr.Key == "property" && attr.Val == prop {
			ok = true
		}
		if attr.Key == "content" {
			content = attr.Val
		}
	}
	return
}
