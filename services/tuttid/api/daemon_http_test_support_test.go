package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	tuttigenerated "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/generated"
)

func performGeneratedRouteRequest(t *testing.T, handler http.Handler, method string, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var requestBody *bytes.Reader
	if body == nil {
		requestBody = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode body: %v", err)
		}
		requestBody = bytes.NewReader(encoded)
	}

	request := httptest.NewRequest(method, path, requestBody)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeGeneratedRouteResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.NewDecoder(recorder.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, recorder.Body.String())
	}
}

func assertGeneratedRouteError(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	code tuttigenerated.ApiErrorDetailsCode,
	reason string,
	developerMessage string,
) {
	t.Helper()

	var response tuttigenerated.ApiErrorResponse
	decodeGeneratedRouteResponse(t, recorder, &response)
	if response.Error.Code != code {
		t.Fatalf("error.code = %q, want %q", response.Error.Code, code)
	}
	if response.Error.Reason == nil || *response.Error.Reason != reason {
		got := "<nil>"
		if response.Error.Reason != nil {
			got = *response.Error.Reason
		}
		t.Fatalf("error.reason = %q, want %q", got, reason)
	}
	if response.Error.DeveloperMessage == nil || *response.Error.DeveloperMessage != developerMessage {
		got := "<nil>"
		if response.Error.DeveloperMessage != nil {
			got = *response.Error.DeveloperMessage
		}
		t.Fatalf("error.developerMessage = %q, want %q", got, developerMessage)
	}
}
