package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/digitaljanitors/go-httpstat"
	"github.com/grafov/m3u8"
	log "github.com/sirupsen/logrus"
)

const VERSION = "0.1.0"

var USER_AGENT string

var client = &http.Client{}

func doRequest(c *http.Client, req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", USER_AGENT)
	resp, err := c.Do(req)
	return resp, err
}

func newRequest(method, url string, stats *httpstat.Result) (*http.Request, error) {
	ctx := httpstat.WithHTTPStat(context.Background(), stats)
	return http.NewRequestWithContext(ctx, method, url, nil)
}

type SegmentDownload struct {
	URI    string
	Limit  int64
	Offset int64
}

func (sd SegmentDownload) SegmentStart() int64 {
	return sd.Offset
}

func (sd SegmentDownload) SegmentEnd() int64 {
	// sd.Offset is the start of the segment
	// sd.Limit is the length of the segment
	// so the last byte we want is 1 less than the sum of Offset & Limit
	return sd.Offset + sd.Limit - 1
}

func NewSegmentDownload(uri string, limit, offset int64) *SegmentDownload {
	return &SegmentDownload{
		URI:    uri,
		Limit:  limit,
		Offset: offset,
	}
}

func translateURI(playlistURL *url.URL, segmentURI string) (string, error) {
	msUrl, err := playlistURL.Parse(segmentURI)
	if err != nil {
		return "", err
	}
	msURI, err := url.QueryUnescape(msUrl.String())
	if err != nil {
		return "", err
	}
	return msURI, nil
}

func downloadSegments(dlc chan *SegmentDownload) {
	tmpfile, err := ioutil.TempFile("", "echo360-benchmark")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name()) // clean up after ourself
	}()

	for v := range dlc {
		stats := &httpstat.Result{}
		req, err := newRequest("GET", v.URI, stats)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", v.SegmentStart(), v.SegmentEnd()))
		resp, err := doRequest(client, req)
		if err != nil {
			log.Print(err)
			continue
		}
		if !(resp.StatusCode >= 200 && resp.StatusCode <= 299) {
			log.Warnf("Recieved HTTP %v for %v @%d-%d\n", resp.StatusCode, v.URI, v.SegmentStart(), v.SegmentEnd())
			continue
		}
		err = resp.Write(ioutil.Discard)
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		stats.End(time.Now())
		log.WithFields(stats.Fields()).Infof("Downloaded %d bytes of %v @%d-%d\n", resp.ContentLength, v.URI, v.SegmentStart(), v.SegmentEnd())

	}

}

func getPlaylist(urlStr string, dlc chan *SegmentDownload) {
	playlistUrl, err := url.Parse(urlStr)
	if err != nil {
		log.Fatal(err)
	}
	for {
		stats := &httpstat.Result{}
		req, err := newRequest("GET", urlStr, stats)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := doRequest(client, req)
		if err != nil {
			log.Print(err)
			time.Sleep(time.Duration(3) * time.Second)
		}
		playlist, listType, err := m3u8.DecodeFrom(resp.Body, true)
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		if listType == m3u8.MEDIA {
			mpl := playlist.(*m3u8.MediaPlaylist)
			if mpl.Map != nil {
				uri, err := translateURI(playlistUrl, mpl.Map.URI)
				if err != nil {
					log.Fatal(err)
				}
				dlc <- NewSegmentDownload(uri, mpl.Map.Limit, mpl.Map.Offset)
			}
			for _, v := range mpl.Segments {
				if v != nil {
					uri, err := translateURI(playlistUrl, v.URI)
					if err != nil {
						log.Print(err)
						continue
					}
					dlc <- NewSegmentDownload(uri, v.Limit, v.Offset)
				}
			}
			if mpl.Closed {
				close(dlc)
				return
			} else {
				log.Print("Sleeping.")
				time.Sleep(time.Duration(int64(mpl.TargetDuration * 1000000000)))
			}
		} else {
			log.Fatal("Not a valid media playlist")
		}
	}

}

func main() {
	playlist := "https://benchmark.echo360.org.au/1/s1q1.m3u8"

	dlChan := make(chan *SegmentDownload, 1024)
	go getPlaylist(playlist, dlChan)
	downloadSegments(dlChan)
}
