package httpd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

const (
	QUERY          = "query"
	SUGGEST_METRIC = "suggestmetric"
	SUGGEST_TAGK   = "suggesttagk"
	SUGGEST_TAGV   = "suggesttagv"
)

func (h *Handler) serveOpenTSDBMetaPut(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePut implements OpenTSDB's HTTP /api/put endpoint.
func (h *Handler) serveOpenTSDBPut(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePut implements OpenTSDB's HTTP /api/query endpoint.
func (h *Handler) serveOpenTSDBQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Require POST method.
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	db := "default"
	keys, ok := r.URL.Query()["tenant"]
	if ok && len(keys) > 0 {
		db = keys[0]
	}

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	// Wrap reader if it's gzip encoded.
	var br *bufio.Reader
	br = bufio.NewReader(r.Body)

	tsQuery := TSQuery{}
	dec := json.NewDecoder(br)
	if err := dec.Decode(&tsQuery); err != nil {
		http.Error(w, "json object decode error", http.StatusBadRequest)
		return
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var qr io.Reader
	queries, err := ConvertInfluxQL(db, &tsQuery)
	if err != nil {
		http.Error(w, "invalid opentsdb query", http.StatusBadRequest)
		return
	}
	qr = strings.NewReader(strings.Join(queries, ";"))

	if qr == nil {
		http.Error(rw, `missing required parameter "query"`, http.StatusBadRequest)
		return
	}

	p := influxql.NewParser(qr)

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		http.Error(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	chunkSize := 10000
	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:  db,
		ChunkSize: chunkSize,
		ReadOnly:  r.Method == "GET",
		NodeID:    nodeID,
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		if notifier, ok := w.(http.CloseNotifier); ok {
			// CloseNotify() is not guaranteed to send a notification when the query
			// is closed. Use this channel to signal that the query is finished to
			// prevent lingering goroutines that may be stuck.
			done := make(chan struct{})
			defer close(done)

			notify := notifier.CloseNotify()
			go func() {
				// Wait for either the request to finish
				// or for the client to disconnect
				select {
				case <-done:
				case <-notify:
					close(closing)
				}
			}()
			opts.AbortCh = done
		} else {
			defer close(closing)
		}
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*query.Result, 0)}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	h.writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// pull all results from the channel
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		//TODO need check "s" or "ms"
		convertToEpoch(r, "s")
		// It's not chunked so buffer results in memory.
		// Results for statements need to be combined together.
		// We need to check if this new result is for the same statement as
		// the last result, or for the next statement
		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			resp.Results = append(resp.Results, r)
		}
	}

	rw.WriteOpenTSDBResponse(resp, &tsQuery, QUERY)
}

func (h *Handler) serveOpenTSDBSuggest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	strs := strings.Split(r.URL.Path, "/")
	suggestType := strs[len(strs)-1]

	var metric string
	if suggestType == SUGGEST_TAGK || suggestType == SUGGEST_TAGV {
		metrics, ok := r.URL.Query()["metric"]
		if !ok || len(metrics) < 0 {
			http.Error(w, "miss metric parameter", http.StatusBadRequest)
		}
		metric = metrics[0]
	}

	var tagk string
	if suggestType == SUGGEST_TAGV {
		tagks, ok := r.URL.Query()["tagk"]
		if !ok || len(tagks) < 0 {
			http.Error(w, "miss metric parameter", http.StatusBadRequest)
		}
		tagk = tagks[0]
	}
	db := "default"
	keys, ok := r.URL.Query()["tenant"]
	if ok && len(keys) > 0 {
		db = keys[0]
	}

	queryName := metric
	if db == "_internal" && metric != "" {
		component := strings.Split(metric, ".")
		queryName = strings.Join(component[0:len(component)-1], ".")
	}

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	qs, hasQ := r.URL.Query()["q"]

	var queryStr string
	if suggestType == SUGGEST_TAGV {
		if hasQ && len(qs) > 0 && qs[0] != "*" {
			queryStr = fmt.Sprintf("SHOW TAG VALUES ON \"%s\" FROM \"%s\" WITH KEY = \"%s\" WHERE \"%s\" =~ /^%s.*/ limit 15 ", db, queryName, tagk, tagk, qs[0])
		} else {
			queryStr = fmt.Sprintf("SHOW TAG VALUES ON \"%s\" FROM \"%s\" WITH KEY = \"%s\" limit 15", db, queryName, tagk)
		}
	} else if suggestType == SUGGEST_TAGK {
		queryStr = fmt.Sprintf("SHOW TAG KEYS ON \"%s\" FROM \"%s\"  limit 15", db, queryName)
	} else if suggestType == SUGGEST_METRIC {
		if hasQ && len(qs) > 0 {
			queryStr = fmt.Sprintf("SHOW MEASUREMENTS ON \"%s\" WITH MEASUREMENT =~ /^%s.*/  limit 15", db, qs[0])
		} else {
			queryStr = fmt.Sprintf("SHOW MEASUREMENTS ON \"%s\" limit 15", db)
		}
	}

	h.Logger.Info("in suggest", zap.String("type", suggestType), zap.String("qstr", queryStr))

	var qr io.Reader
	//fmt.Println(queryStr)
	qr = strings.NewReader(queryStr)

	if qr == nil {
		http.Error(rw, `missing required parameter "query"`, http.StatusBadRequest)
		return
	}

	p := influxql.NewParser(qr)

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		http.Error(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	opts := query.ExecutionOptions{
		Database:  db,
		ChunkSize: 10000,
		ReadOnly:  r.Method == "GET",
		NodeID:    0,
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	closing = make(chan struct{})
	if notifier, ok := w.(http.CloseNotifier); ok {
		// CloseNotify() is not guaranteed to send a notification when the query
		// is closed. Use this channel to signal that the query is finished to
		// prevent lingering goroutines that may be stuck.
		done := make(chan struct{})
		defer close(done)

		notify := notifier.CloseNotify()
		go func() {
			// Wait for either the request to finish
			// or for the client to disconnect
			select {
			case <-done:
			case <-notify:
				close(closing)
			}
		}()
		opts.AbortCh = done
	} else {
		defer close(closing)
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*query.Result, 0)}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	h.writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	for r := range results {
		if r == nil {
			continue
		}

		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			resp.Results = append(resp.Results, r)
		}
	}

	rw.WriteOpenTSDBResponse(resp, nil, suggestType)
}

// chanListener represents a listener that receives connections through a channel.
type chanListener struct {
	addr   net.Addr
	ch     chan net.Conn
	done   chan struct{}
	closer sync.Once // closer ensures that Close is idempotent.
}

// newChanListener returns a new instance of chanListener.
func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{
		addr: addr,
		ch:   make(chan net.Conn),
		done: make(chan struct{}),
	}
}

func (ln *chanListener) Accept() (net.Conn, error) {
	errClosed := errors.New("network connection closed")
	select {
	case <-ln.done:
		return nil, errClosed
	case conn, ok := <-ln.ch:
		if !ok {
			return nil, errClosed
		}
		return conn, nil
	}
}

// Close closes the connection channel.
func (ln *chanListener) Close() error {
	ln.closer.Do(func() {
		close(ln.done)
	})
	return nil
}

// Addr returns the network address of the listener.
func (ln *chanListener) Addr() net.Addr { return ln.addr }

// readerConn represents a net.Conn with an assignable reader.
type readerConn struct {
	net.Conn
	r io.Reader
}

// Read implements the io.Reader interface.
func (conn *readerConn) Read(b []byte) (n int, err error) { return conn.r.Read(b) }

// point represents an incoming JSON data point.
type point struct {
	Metric string            `json:"metric"`
	Time   int64             `json:"timestamp"`
	Value  float64           `json:"value"`
	Tags   map[string]string `json:"tags,omitempty"`
}

//convert opentsdb query protocol to influxql
func ConvertInfluxQL(db string, tsQuery *TSQuery) ([]string, error) {

	querys := make([]string, len(tsQuery.TsSubQuery))
	for index, subQuery := range tsQuery.TsSubQuery {
		queryName := "v0"
		queryField := "value"
		if db == "_internal" {
			component := strings.Split(subQuery.Metric, ".")
			queryField = component[len(component)-1]
			queryName = strings.Join(component[0:len(component)-1], ".")
		} else {
			queryName = subQuery.Metric
		}

		// Convert opentsdb aggregator type avg to mean
		if subQuery.Aggregator == "avg" {
			subQuery.Aggregator = "mean"
		}

		if subQuery.Rate {
			subQuery.Aggregator = "derivative"
		}

		querySql := fmt.Sprintf("select %s(%s) from \"%s\"", subQuery.Aggregator, queryField, queryName)
		var groupBy []string
		var tagComponent []string
		if len(subQuery.Tags) > 0 {
			for k, v := range subQuery.Tags {
				if !strings.Contains(v, "*") {
					tagComponent = append(tagComponent, fmt.Sprintf("\"%s\"='%s'", k, v))
				} else {
					groupBy = append(groupBy, "\""+k+"\"")
				}
			}
		}

		querySql += fmt.Sprintf(" where time > %dms and time < %dms", tsQuery.Start, tsQuery.End)
		if len(tagComponent) > 0 {
			querySql += " and " + strings.Join(tagComponent, " and ")
		}

		if len(groupBy) > 0 {
			querySql += " GROUP BY " + strings.Join(groupBy, ", ")
		}

		granularity := "1s"
		if subQuery.Granularity != "" {
			granularity = subQuery.Granularity
		}
		if !subQuery.Rate {
			if len(groupBy) > 0 {
				querySql += ", time(" + granularity + ") fill(none) "
			} else {
				querySql += " GROUP BY time(" + granularity + ") fill(none) "
			}
		}

		querys[index] = querySql
	}

	return querys, nil
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
//func convertToEpoch(r *query.Result) {
//	divisor := int64(1)
//	divisor = int64(time.Second)
//
//	for _, s := range r.Series {
//		for _, v := range s.Values {
//			if ts, ok := v[0].(time.Time); ok {
//				v[0] = ts.UnixNano() / divisor
//			}
//		}
//	}
//}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
//func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
//	w.WriteHeader(code)
//}

// Response represents a list of statement results.
type OpenTSDBResponse struct {
	Results []*query.Result
	Err     error
}

type TSQuery struct {
	Start      int64        `json:"start"`
	End        int64        `json:"end"`
	Tenant     string       `json:"tenant"`
	TsSubQuery []TSSubQuery `json:"queries"`
}

type TSSubQuery struct {
	Metric      string            `json:"metric"`
	Aggregator  string            `json:"aggregator"`
	Rate        bool              `json:"rate"`
	Tags        map[string]string `json:"tags"`
	Granularity string            `json:"granularity"`
}

type SimpleSpan struct {
	Metric string                 `json:"metric"`
	Tags   map[string]string      `json:"tags"`
	Dps    map[string]interface{} `json:"dps"`
}
