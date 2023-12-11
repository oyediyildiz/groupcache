/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/oyediyildiz/groupcache/v2/consistenthash"
	pb "github.com/oyediyildiz/groupcache/v2/groupcachepb"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	mu          sync.Mutex // guards the peerPickers map
	peerPickers map[string]*HTTPPeerPicker

	// opts specifies the options.
	opts HTTPPoolOptions
}

type HTTPPeerPicker struct {
	pool        *HTTPPool
	mu          sync.Mutex // guards peers and httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, uses the http.Request.Context()
	Context func(*http.Request) context.Context

	PerGroupPeerPicker bool
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	opts := HTTPPoolOptions{}
	if o != nil {
		opts = *o
	}
	if opts.BasePath == "" {
		opts.BasePath = defaultBasePath
	}
	if opts.Replicas == 0 {
		opts.Replicas = defaultReplicas
	}

	p := &HTTPPool{
		opts:        opts,
		self:        self,
		peerPickers: make(map[string]*HTTPPeerPicker),
	}

	if opts.PerGroupPeerPicker {
		RegisterPerGroupPeerPicker(func(groupName string) PeerPicker {
			return p.getPeerPicker(groupName)
		})
	} else {
		pp := p.createPerGroupPeerPicker(opts.Replicas, opts.HashFn)
		p.peerPickers["default"] = pp
		RegisterPeerPicker(func() PeerPicker { return pp })
	}

	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPeerPicker) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			getTransport: p.pool.opts.Transport,
			baseURL:      peer + p.pool.opts.BasePath,
		}
	}
}

// GetAll returns all the peers in the pool
func (p *HTTPPeerPicker) GetAll() []ProtoGetter {
	p.mu.Lock()
	defer p.mu.Unlock()

	var i int
	res := make([]ProtoGetter, len(p.httpGetters))
	for _, v := range p.httpGetters {
		res[i] = v
		i++
	}
	return res
}

func (p *HTTPPeerPicker) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.pool.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) Set(peers ...string) {
	p.peerPickers["default"].Set(peers...)
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	return p.peerPickers["default"].PickPeer(key)
}

func (p *HTTPPool) GetAll() []ProtoGetter {
	return p.peerPickers["default"].GetAll()
}

func (p *HTTPPool) createPerGroupPeerPicker(replicas int, hashFn consistenthash.Hash) *HTTPPeerPicker {
	return &HTTPPeerPicker{
		pool:        p,
		peers:       consistenthash.New(replicas, hashFn),
		httpGetters: make(map[string]*httpGetter),
	}
}

func (p *HTTPPool) getPeerPicker(groupName string) *HTTPPeerPicker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pp, ok := p.peerPickers[groupName]; ok {
		return pp
	}

	pp := p.createPerGroupPeerPicker(p.opts.Replicas, p.opts.HashFn)
	p.peerPickers[groupName] = pp
	return pp
}

func (p *HTTPPool) SetGroupPeers(groupName string, peers ...string) {
	if !p.opts.PerGroupPeerPicker {
		groupName = "default"
	}
	p.getPeerPicker(groupName).Set(peers...)
}

func (p *HTTPPool) PickGroupPeer(groupName string, key string) (ProtoGetter, bool) {
	if !p.opts.PerGroupPeerPicker {
		groupName = "default"
	}
	return p.getPeerPicker(groupName).PickPeer(key)
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.opts.Context != nil {
		ctx = p.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	group.Stats.ServerRequests.Add(1)

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.localRemove(key)
		return
	}

	// The read the body and set the key value
	if r.Method == http.MethodPut {
		defer r.Body.Close()
		b := bufferPool.Get().(*bytes.Buffer)
		b.Reset()
		defer bufferPool.Put(b)
		_, err := io.Copy(b, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var out pb.SetRequest
		err = proto.Unmarshal(b.Bytes(), &out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var expire time.Time
		if out.Expire != nil && *out.Expire != 0 {
			expire = time.Unix(*out.Expire/int64(time.Second), *out.Expire%int64(time.Second))
		}

		group.localSet(*out.Key, out.Value, expire, &group.mainCache)
		return
	}

	var b []byte

	value := AllocatingByteSliceSink(&b)
	err := group.Get(ctx, key, value)
	if err != nil {
		if errors.Is(err, &ErrNotFound{}) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	view, err := value.view()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var expireNano int64
	if !view.e.IsZero() {
		expireNano = view.Expire().UnixNano()
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: b, Expire: &expireNano})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	baseURL      string
}

func (p *httpGetter) GetURL() string {
	return p.baseURL
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

type request interface {
	GetGroup() string
	GetKey() string
}

func (h *httpGetter) makeRequest(ctx context.Context, m string, in request, b io.Reader, out *http.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.PathEscape(in.GetGroup()),
		url.PathEscape(in.GetKey()),
	)
	req, err := http.NewRequestWithContext(ctx, m, u, b)
	if err != nil {
		return fmt.Errorf("failed to create new request: %w", err)
	}

	tr := http.DefaultTransport
	if h.getTransport != nil {
		tr = h.getTransport(ctx)
	}

	res, err := tr.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("failed to roundtrip: %w", err)
	}
	*out = *res
	return nil
}

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, nil, &res); err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// Limit reading the error body to max 1 MiB
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 1024*1024))

		if res.StatusCode == http.StatusNotFound {
			return &ErrNotFound{Msg: strings.Trim(string(msg), "\n")}
		}

		if res.StatusCode == http.StatusServiceUnavailable {
			return &ErrRemoteCall{Msg: strings.Trim(string(msg), "\n")}
		}

		return fmt.Errorf("server returned: %v, %v", res.Status, string(msg))
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err := io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}
	return nil
}

func (h *httpGetter) Set(ctx context.Context, in *pb.SetRequest) error {
	body, err := proto.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal: set request %w", err)
	}
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodPut, in, bytes.NewReader(body), &res); err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body (status=%v): %v", res.Status, err)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}

func (h *httpGetter) Remove(ctx context.Context, in *pb.GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, nil, &res); err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body (status=%v): %w", res.Status, err)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}
