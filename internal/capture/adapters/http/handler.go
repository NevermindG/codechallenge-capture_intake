package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/NevermindG/309-capture-intake/internal/capture/application/intake"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type Handler struct {
	intake        *intake.Service
	reader        intake.CaptureReader
	maxBodyBytes  int64
	maxImageBytes int64
	logger        *slog.Logger
}

func NewHandler(service *intake.Service, reader intake.CaptureReader, maxBodyBytes, maxImageBytes int64, logger *slog.Logger) *Handler {
	return &Handler{intake: service, reader: reader, maxBodyBytes: maxBodyBytes, maxImageBytes: maxImageBytes, logger: logger}
}

func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Post("/v1/captures", h.createCapture)
	r.Get("/v1/captures/{key}", h.getCapture)
	return r
}

func (h *Handler) createCapture(w http.ResponseWriter, r *http.Request) {
	requestID := requestID(r)
	w.Header().Set("X-Request-ID", requestID)
	logger := h.logger.With(slog.String("requestId", requestID))
	ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, apiError{HTTPCode: http.StatusBadRequest, Code: 400001, ErrorClass: "IdempotencyKeyMissing", Message: "Idempotency-Key header is required"})
		return
	}
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		writeError(w, apiError{HTTPCode: http.StatusBadRequest, Code: 400002, ErrorClass: "IdempotencyKeyInvalid", Message: err.Error()})
		return
	}
	if r.ContentLength > h.maxBodyBytes {
		writeError(w, apiError{HTTPCode: http.StatusRequestEntityTooLarge, Code: 413001, ErrorClass: "RequestTooLarge", Message: "request body exceeds the configured limit"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	manifest, image, err := parseMultipart(r, h.maxImageBytes)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, apiError{HTTPCode: http.StatusRequestEntityTooLarge, Code: 413001, ErrorClass: "RequestTooLarge", Message: "request body exceeds the configured limit"})
			return
		}
		switch {
		case errors.Is(err, errImageTooLarge):
			writeError(w, apiError{HTTPCode: http.StatusRequestEntityTooLarge, Code: 413002, ErrorClass: "ImageTooLarge", Message: "image exceeds the configured limit"})
		case errors.Is(err, errImageInvalid):
			writeError(w, apiError{HTTPCode: http.StatusUnprocessableEntity, Code: 422002, ErrorClass: "ImageInvalid", Message: err.Error()})
		case errors.Is(err, errManifestMissing):
			writeError(w, apiError{HTTPCode: http.StatusUnprocessableEntity, Code: 422001, ErrorClass: "ManifestMissing", Message: "manifest part is required"})
		case errors.Is(err, errManifestInvalid):
			writeError(w, apiError{HTTPCode: http.StatusUnprocessableEntity, Code: 422003, ErrorClass: "ManifestInvalid", Message: "manifest is invalid"})
		case errors.Is(err, errFrameMissing):
			writeError(w, apiError{HTTPCode: http.StatusUnprocessableEntity, Code: 422004, ErrorClass: "FrameMissing", Message: "frame part is required"})
		default:
			writeError(w, apiError{HTTPCode: http.StatusBadRequest, Code: 400003, ErrorClass: "MultipartMalformed", Message: err.Error()})
		}
		return
	}

	result, err := h.intake.Accept(ctx, key, manifest, bytes.NewReader(image))
	if err != nil {
		logger.Error("capture intake failed", slog.String("error", err.Error()))
		switch {
		case errors.Is(err, domain.ErrInvalidIdempotencyKey):
			writeError(w, apiError{HTTPCode: http.StatusBadRequest, Code: 400002, ErrorClass: "IdempotencyKeyInvalid", Message: err.Error()})
		case errors.Is(err, domain.ErrInvalidManifest):
			writeError(w, apiError{HTTPCode: http.StatusUnprocessableEntity, Code: 422003, ErrorClass: "ManifestInvalid", Message: err.Error()})
		default:
			writeError(w, apiError{HTTPCode: http.StatusServiceUnavailable, Code: 503001, ErrorClass: "CaptureStorageUnavailable", Message: "capture could not be stored; retry the request"})
		}
		return
	}

	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, newCaptureResponse(result.Capture, result.Duplicate))
}

func (h *Handler) getCapture(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		writeError(w, apiError{HTTPCode: http.StatusBadRequest, Code: 400002, ErrorClass: "IdempotencyKeyInvalid", Message: err.Error()})
		return
	}
	capture, err := h.reader.FindByIdempotencyKey(r.Context(), key)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, apiError{HTTPCode: http.StatusNotFound, Code: 404001, ErrorClass: "CaptureNotFound", Message: "capture not found"})
			return
		}
		writeError(w, apiError{HTTPCode: http.StatusServiceUnavailable, Code: 503001, ErrorClass: "CaptureStorageUnavailable", Message: "capture could not be read; retry the request"})
		return
	}
	if capture == nil {
		writeError(w, apiError{HTTPCode: http.StatusNotFound, Code: 404001, ErrorClass: "CaptureNotFound", Message: "capture not found"})
		return
	}
	writeJSON(w, http.StatusOK, newCaptureResponse(*capture, false))
}

func parseMultipart(r *http.Request, maxImageBytes int64) (domain.Manifest, []byte, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return domain.Manifest{}, nil, fmt.Errorf("request must be multipart/form-data: %w", err)
	}
	var (
		manifest     domain.Manifest
		manifestSeen bool
		frameSeen    bool
		sawPart      bool
		image        []byte
	)
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return domain.Manifest{}, nil, fmt.Errorf("read multipart part: %w", err)
		}
		sawPart = true
		switch part.FormName() {
		case "manifest":
			if manifestSeen {
				return domain.Manifest{}, nil, fmt.Errorf("duplicate manifest part")
			}
			manifestSeen = true
			if err := decodeManifest(part, &manifest); err != nil {
				return domain.Manifest{}, nil, errors.Join(err, errManifestInvalid)
			}
		case "frame":
			if frameSeen {
				return domain.Manifest{}, nil, fmt.Errorf("duplicate frame part")
			}
			frameSeen = true
			data, err := readAtMost(part, maxImageBytes+1)
			if err != nil {
				return domain.Manifest{}, nil, fmt.Errorf("read frame: %w", err)
			}
			if int64(len(data)) > maxImageBytes {
				return domain.Manifest{}, nil, errImageTooLarge
			}
			contentType := http.DetectContentType(data)
			if len(data) == 0 || !strings.HasPrefix(contentType, "image/") {
				return domain.Manifest{}, nil, errImageInvalid
			}
			image = data
		}
		if err := drainPart(part); err != nil {
			return domain.Manifest{}, nil, fmt.Errorf("drain multipart part: %w", err)
		}
	}
	if !sawPart {
		return domain.Manifest{}, nil, fmt.Errorf("multipart body contains no parts")
	}
	if !manifestSeen {
		return domain.Manifest{}, nil, errManifestMissing
	}
	if !frameSeen {
		return domain.Manifest{}, nil, errFrameMissing
	}
	if err := manifest.Validate(); err != nil {
		return domain.Manifest{}, nil, errors.Join(err, errManifestInvalid)
	}
	return manifest, image, nil
}

func decodeManifest(r io.Reader, out *domain.Manifest) error {
	// Pointer fields preserve presence, so amount="0" is valid while omitted amount is rejected.
	type payload struct {
		CapturedAt *time.Time      `json:"capturedAt"`
		Amount     *domain.Decimal `json:"amount"`
		Currency   *string         `json:"currency"`
	}
	dec := json.NewDecoder(io.LimitReader(r, 512*1024))
	dec.DisallowUnknownFields()
	var p payload
	if err := dec.Decode(&p); err != nil {
		return fmt.Errorf("manifest must be a JSON object: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("manifest must contain exactly one JSON value")
		}
		return fmt.Errorf("manifest has trailing data: %w", err)
	}
	if p.CapturedAt == nil || p.Amount == nil || p.Currency == nil {
		return fmt.Errorf("manifest requires capturedAt, amount, and currency")
	}
	*out = domain.Manifest{CapturedAt: p.CapturedAt.UTC(), Amount: *p.Amount, Currency: *p.Currency}
	return nil
}

func readAtMost(r io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}

func drainPart(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

type requestIDKey struct{}

func requestID(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Request-ID")); v != "" && len(v) <= 128 {
		return v
	}
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}

type apiError struct {
	Message    string `json:"message"`
	Code       int    `json:"code"`
	ErrorClass string `json:"errorClass"`
	HTTPCode   int    `json:"httpCode"`
}

func writeError(w http.ResponseWriter, err apiError) {
	writeJSON(w, err.HTTPCode, err)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type captureResponse struct {
	ID             string `json:"id"`
	IdempotencyKey string `json:"idempotencyKey"`
	CapturedAt     string `json:"capturedAt"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
	ImageKey       string `json:"imageKey"`
	CreatedAt      string `json:"createdAt"`
	Duplicate      bool   `json:"duplicate"`
}

func newCaptureResponse(c domain.Capture, duplicate bool) captureResponse {
	return captureResponse{
		ID: c.ID.String(), IdempotencyKey: c.IdempotencyKey, CapturedAt: c.CapturedAt.UTC().Format(time.RFC3339Nano),
		Amount: c.Amount.String(), Currency: c.Currency, ImageKey: c.ImageKey,
		CreatedAt: c.CreatedAt.UTC().Format(time.RFC3339Nano), Duplicate: duplicate,
	}
}

var (
	errManifestMissing = errors.New("manifest part missing")
	errManifestInvalid = errors.New("manifest invalid")
	errFrameMissing    = errors.New("frame part missing")
	errImageTooLarge   = errors.New("image too large")
	errImageInvalid    = errors.New("image bytes are not a supported image type")
)
