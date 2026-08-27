package gateway

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"sync"
	"time"
)

// LocalCA issues per-room certificates for *.tutti. Trust exists only
// inside Open Tutti's own runtimes — the Tutti Browser and agent/terminal
// session containers get the CA bundle injected; the host OS certificate
// store never sees it, so system Chrome/Safari keep refusing .tutti.
type LocalCA struct {
	mu        sync.Mutex
	caCert    *x509.Certificate
	caKey     *ecdsa.PrivateKey
	leafCache map[string]tls.Certificate
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
	return &LocalCA{caCert: cert, caKey: key, leafCache: map[string]tls.Certificate{}}, nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if cert, ok := c.leafCache[host]; ok {
		return cert, nil
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
	c.leafCache[host] = cert
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
