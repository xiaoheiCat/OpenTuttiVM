package room

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// argon2Params are the room-password hashing parameters. Only the encoded
// hash is stored, never the plaintext password.
type argon2Params struct {
	time    uint32
	memory  uint32
	threads uint8
	keyLen  uint32
	saltLen uint32
}

var defaultArgon2 = argon2Params{time: 1, memory: 64 * 1024, threads: 2, keyLen: 32, saltLen: 16}

// HashRoomPassword derives an Argon2id hash encoded as
// argon2id$t,m,p$saltB64$hashB64.
func HashRoomPassword(password string) (string, error) {
	p := defaultArgon2
	salt := randomBytes(p.saltLen)
	key := argon2.IDKey([]byte(password), salt, p.time, p.memory, p.threads, p.keyLen)
	return fmt.Sprintf("argon2id$%d,%d,%d$%s$%s",
		p.time, p.memory, p.threads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// VerifyRoomPassword checks a password against an encoded Argon2id hash in
// constant time.
func VerifyRoomPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "argon2id" {
		return false
	}
	var time_, memory uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[1], "%d,%d,%d", &time_, &memory, &threads); err != nil {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, time_, memory, threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func randomBytes(n uint32) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
