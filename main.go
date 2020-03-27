package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/digitaljanitors/go-httpstat"
	"github.com/grafov/m3u8"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const VERSION = "0.1.0"

var USER_AGENT = fmt.Sprintf("HLS-Benchmark-tool/%s", VERSION)

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
	URI      string
	Duration float64
	Limit    int64
	Offset   int64
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

func NewSegmentDownload(uri string, duration float64, limit, offset int64) *SegmentDownload {
	return &SegmentDownload{
		URI:      uri,
		Duration: duration,
		Limit:    limit,
		Offset:   offset,
	}
}

type ResultSummary struct {
	// The following are duration for each phase
	DNSLookup        []time.Duration
	TCPConnection    []time.Duration
	TLSHandshake     []time.Duration
	ServerProcessing []time.Duration
	ContentTransfer  []time.Duration

	// The followings are timeline of request
	NameLookup    []time.Duration
	Connect       []time.Duration
	Pretransfer   []time.Duration
	StartTransfer []time.Duration
	Total         []time.Duration
}

func (rs *ResultSummary) Add(result *httpstat.Result) {
	rs.DNSLookup = append(rs.DNSLookup, result.DNSLookup)
	rs.TCPConnection = append(rs.TCPConnection, result.TCPConnection)
	rs.TLSHandshake = append(rs.TLSHandshake, result.TLSHandshake)
	rs.ServerProcessing = append(rs.ServerProcessing, result.ServerProcessing)
	rs.ContentTransfer = append(rs.ContentTransfer, result.ContentTransfer)
	rs.NameLookup = append(rs.NameLookup, result.NameLookup)
	rs.Connect = append(rs.Connect, result.Connect)
	rs.Pretransfer = append(rs.Pretransfer, result.Pretransfer)
	rs.StartTransfer = append(rs.StartTransfer, result.StartTransfer)
	rs.Total = append(rs.Total, result.Total)
}

func (rs *ResultSummary) Averages() map[string]interface{} {
	var f = func(d []time.Duration) time.Duration {
		var total time.Duration
		for _, value := range d {
			total += value
		}
		return time.Duration(int64(total) / int64(len(d)))
	}
	return map[string]interface{}{
		"DNSLookup":        f(rs.DNSLookup),
		"TCPConnection":    f(rs.TCPConnection),
		"TLSHandshake":     f(rs.TLSHandshake),
		"ServerProcessing": f(rs.ServerProcessing),
		"ContentTransfer":  f(rs.ContentTransfer),

		"NameLookup":    f(rs.NameLookup),
		"Connect":       f(rs.Connect),
		"Pretransfer":   f(rs.Connect),
		"StartTransfer": f(rs.StartTransfer),
		"Total":         f(rs.Total),
	}
}

func (rs *ResultSummary) Maximums() map[string]interface{} {
	var f = func(d []time.Duration) time.Duration {
		var max time.Duration
		for _, value := range d {
			if value > max {
				max = value
			}
		}
		return max
	}
	return map[string]interface{}{
		"DNSLookup":        f(rs.DNSLookup),
		"TCPConnection":    f(rs.TCPConnection),
		"TLSHandshake":     f(rs.TLSHandshake),
		"ServerProcessing": f(rs.ServerProcessing),
		"ContentTransfer":  f(rs.ContentTransfer),

		"NameLookup":    f(rs.NameLookup),
		"Connect":       f(rs.Connect),
		"Pretransfer":   f(rs.Connect),
		"StartTransfer": f(rs.StartTransfer),
		"Total":         f(rs.Total),
	}
}

func (rs *ResultSummary) Minimums() map[string]interface{} {
	var f = func(d []time.Duration) time.Duration {
		var min time.Duration
		for _, value := range d {
			if value < min {
				min = value
			}
		}
		return min
	}
	return map[string]interface{}{
		"DNSLookup":        f(rs.DNSLookup),
		"TCPConnection":    f(rs.TCPConnection),
		"TLSHandshake":     f(rs.TLSHandshake),
		"ServerProcessing": f(rs.ServerProcessing),
		"ContentTransfer":  f(rs.ContentTransfer),

		"NameLookup":    f(rs.NameLookup),
		"Connect":       f(rs.Connect),
		"Pretransfer":   f(rs.Connect),
		"StartTransfer": f(rs.StartTransfer),
		"Total":         f(rs.Total),
	}
}

func (rs *ResultSummary) LogSummary() {
	log.WithFields(rs.Minimums()).Info("Results Minimums")
	log.WithFields(rs.Maximums()).Info("Results Maximums")
	log.WithFields(rs.Averages()).Info("Results Averages")
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

func calculateTransfer(bytesDownloaded int64, overTime time.Duration) string {
	// (bytes downloaded / over time) = Bytes/second
	// Bytes/second x 0.000008 = Mb/s
	// rate := float64(bytesDownloaded) / overTime.Seconds()
	rate := float64(bytesDownloaded) / overTime.Seconds() * 0.000008
	return fmt.Sprintf("%.2f Mb/s", rate)
}

func logSegmentDownload(resp *http.Response, stats *httpstat.Result, segment *SegmentDownload) {
	lvl := logrus.InfoLevel
	sd := time.Duration(int64(segment.Duration) * int64(time.Second))
	if stats.Total >= sd {
		lvl = logrus.WarnLevel
	}
	log.WithFields(stats.Fields()).
		WithField("X-Cache", resp.Header["X-Cache"][0]).
		WithField("TransferRate", calculateTransfer(resp.ContentLength, stats.ContentTransfer)).
		WithField("ConnectedTo", stats.ConnectedTo).
		Logf(lvl, "Downloaded %d bytes of %v @%d-%d\n", resp.ContentLength, segment.URI, segment.SegmentStart(), segment.SegmentEnd())
}

func downloadSegments(dlc chan *SegmentDownload) ResultSummary {
	results := ResultSummary{}

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
		logSegmentDownload(resp, stats, v)
		results.Add(stats)
	}

	return results
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
		stats.End(time.Now())
		logSegmentDownload(resp, stats, &SegmentDownload{urlStr, 1, 0, 1})
		if listType == m3u8.MEDIA {
			mpl := playlist.(*m3u8.MediaPlaylist)
			if mpl.Map != nil {
				uri, err := translateURI(playlistUrl, mpl.Map.URI)
				if err != nil {
					log.Fatal(err)
				}
				dlc <- NewSegmentDownload(uri, mpl.TargetDuration, mpl.Map.Limit, mpl.Map.Offset)
			}
			for _, v := range mpl.Segments {
				if v != nil {
					uri, err := translateURI(playlistUrl, v.URI)
					if err != nil {
						log.Print(err)
						continue
					}
					dlc <- NewSegmentDownload(uri, v.Duration, v.Limit, v.Offset)
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
	flag.Parse()

	if flag.NArg() < 1 {
		os.Stderr.Write([]byte("Usage: hlsbenchmark media-playlist-url\n"))
		flag.PrintDefaults()
		os.Exit(2)
	}

	dlChan := make(chan *SegmentDownload, 1024)
	go getPlaylist(flag.Arg(0), dlChan)
	results := downloadSegments(dlChan)
	results.LogSummary()
}
