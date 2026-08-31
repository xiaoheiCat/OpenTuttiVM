package gateway

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	vmprotocol "github.com/xiaoheiCat/OpenTuttiVM/packages/workspace/vm-protocol"
)

// LocalCA issues per-room certificates for *.tutti. Trust exists only
// inside Open Tutti's own runtimes — the Tutti Browser and agent/terminal
// session containers get the CA bundle injected; the host OS certificate
// store never sees it, so system Chrome/Safari keep refusing .tutti.
type LocalCA struct {
	mu        sync.Mutex
	caCert    *x509.Certificate
	caKey     *ecdsa.PrivateKey
	leafCache map[string]cachedLeaf
}

type cachedLeaf struct {
	cert   tls.Certificate
	issued time.Time
}

const (
	maxLeafCache = 256
	leafCacheTTL = 24 * time.Hour
)

// LoadOrCreateLocalCA loads the persisted room CA from dir (room-ca.pem
// + room-ca-key.pem) or generates and persists a fresh one. Persisting
// matters: a regenerated CA invalidates every previously issued .tutti
// certificate, so after a room-sync restart all consumers still
// trusting the old bundle would reject the new certificates until each
// of them reloads. The key file stays device-private (0600).
// keyMatchesCert reports whether the private key actually corresponds
// to the certificate's public key (a torn write can persist a new
// certificate with the previous key).
func keyMatchesCert(ca *LocalCA) bool {
	if ca.caKey == nil || ca.caCert == nil {
		return false
	}
	want, err := x509.MarshalECPrivateKey(ca.caKey)
	if err != nil {
		return false
	}
	k, err := x509.ParseECPrivateKey(want)
	if err != nil {
		return false
	}
	return publicKeysEqual(&k.PublicKey, ca.caCert.PublicKey)
}

func publicKeysEqual(a, b any) bool {
	ab, aerr := x509.MarshalPKIXPublicKey(a)
	bb, berr := x509.MarshalPKIXPublicKey(b)
	return aerr == nil && berr == nil && bytes.Equal(ab, bb)
}

func LoadOrCreateLocalCA(dir string) (*LocalCA, error) {
	certPEM, certErr := os.ReadFile(filepath.Join(dir, "room-ca.pem"))
	keyPEM, keyErr := os.ReadFile(filepath.Join(dir, "room-ca-key.pem"))
	if certErr == nil && keyErr == nil {
		if ca, err := parseCAPair(certPEM, keyPEM); err == nil && keyMatchesCert(ca) {
			return ca, nil
		}
		// Unreadable or MISMATCHED pair: a host interrupted between
		// the two writes leaves a new certificate beside an old key;
		// parsing each independently would accept a pair whose every
		// later issuance and handshake fails. Fall through and
		// regenerate atomically.
	}
	ca, err := NewLocalCA()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "room-ca.pem"), ca.CACertPEM(), 0o600); err != nil {
		return nil, err
	}
	keyBytes, err := x509.MarshalECPrivateKey(ca.caKey)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "room-ca-key.pem"),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		return nil, err
	}
	return ca, nil
}

func parseCAPair(certPEM, keyPEM []byte) (*LocalCA, error) {
	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)
	if certBlock == nil || keyBlock == nil {
		return nil, errors.New("room CA pair not PEM-encoded")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, err
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}
	return &LocalCA{caCert: cert, caKey: key, leafCache: map[string]cachedLeaf{}}, nil
}

// NewLocalCA generates a fresh Ed25519-equivalent (ECDSA P-256) CA. One CA
// per installed room runtime; the key lives in device-private storage.
func NewLocalCA() (*LocalCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "OpenTuttiVM Room CA", Organization: []string{"OpenTuttiVM"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(5, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &LocalCA{caCert: cert, caKey: key, leafCache: map[string]cachedLeaf{}}, nil
}

// CACertPEM exports the CA certificate for injecting into the Tutti
// Browser and session containers.
func (c *LocalCA) CACertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.caCert.Raw})
}

// LeafFor returns a TLS certificate for one .tutti host (and its wildcard
// parent), signing on demand and caching. Leaves cover the host and the
// wildcard so session and device names share the cert where possible.
func (c *LocalCA) LeafFor(host string) (tls.Certificate, error) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if _, err := vmprotocol.ParseTuttiHost(host); err != nil {
		return tls.Certificate{}, fmt.Errorf("invalid tutti TLS host %q: %w", host, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if cached, ok := c.leafCache[host]; ok && now.Sub(cached.issued) < leafCacheTTL {
		return cached.cert, nil
	}
	if len(c.leafCache) >= maxLeafCache {
		var oldestHost string
		var oldest time.Time
		for cachedHost, cached := range c.leafCache {
			if now.Sub(cached.issued) >= leafCacheTTL {
				delete(c.leafCache, cachedHost)
				continue
			}
			if oldestHost == "" || cached.issued.Before(oldest) {
				oldestHost, oldest = cachedHost, cached.issued
			}
		}
		if len(c.leafCache) >= maxLeafCache && oldestHost != "" {
			delete(c.leafCache, oldestHost)
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tpl := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host, "*." + parentDomain(host)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, c.caCert, &key.PublicKey, c.caKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	cert := tls.Certificate{Certificate: [][]byte{leaf.Raw, c.caCert.Raw}, PrivateKey: key}
	c.leafCache[host] = cachedLeaf{cert: cert, issued: now}
	return cert, nil
}

// TLSConfigFor builds a server TLS config with on-demand .tutti signing.
func (c *LocalCA) TLSConfigFor(host string) (*tls.Config, error) {
	cert, err := c.LeafFor(host)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// VerifyInTuttiRuntime builds a client cert pool that trusts only the room
// CA — the trust model injected into the Tutti Browser and containers.
func (c *LocalCA) VerifyInTuttiRuntime() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(c.caCert)
	return pool
}

func parentDomain(host string) string {
	for i := 0; i < len(host); i++ {
		if host[i] == '.' && i+1 < len(host) {
			return host[i+1:]
		}
	}
	return host
}

func randomSerial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 120)
	n, err := rand.Int(rand.Reader, limit)
	if err != nil {
		panic(err)
	}
	return n
}
